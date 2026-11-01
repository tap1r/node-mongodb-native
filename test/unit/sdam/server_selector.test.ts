import { expect } from 'chai';

import {
  applyUserServerSelector,
  Binary,
  DeprioritizedServers,
  Long,
  MIN_SECONDARY_WRITE_WIRE_VERSION,
  MongoClient,
  MongoInvalidArgumentError,
  MongoNetworkError,
  MongoParseError,
  MongoServerError,
  MongoServerSelectionError,
  ObjectId,
  parseOptions,
  ReadPreference,
  readPreferenceServerSelector,
  sameServerSelector,
  secondaryWritableServerSelector,
  ServerDescription,
  type ServerSelectorFunction,
  Timestamp,
  TopologyDescription,
  TopologyType,
  writableServerSelector
} from '../../mongodb';
import { topologyWithPlaceholderClient } from '../../tools/utils';

const LOCAL_THRESHOLD_MS = 15;

function member(
  address: string,
  type: 'primary' | 'secondary' | 'mongos' | 'standalone' | 'loadBalancer',
  roundTripTime: number
): ServerDescription {
  if (type === 'mongos') {
    return new ServerDescription(address, { msg: 'isdbgrid', ok: 1 }, { roundTripTime });
  }
  if (type === 'loadBalancer') {
    return new ServerDescription(address, { ok: 1 }, { loadBalanced: true, roundTripTime });
  }
  if (type === 'secondary') {
    return new ServerDescription(
      address,
      { setName: 'rs', secondary: true, ok: 1 },
      { roundTripTime }
    );
  }
  return new ServerDescription(
    address,
    { setName: type === 'primary' ? 'rs' : undefined, isWritablePrimary: true, ok: 1 },
    { roundTripTime }
  );
}

function description(
  type: TopologyType,
  servers: ServerDescription[],
  localThresholdMS = LOCAL_THRESHOLD_MS
): TopologyDescription {
  return new TopologyDescription(
    type,
    new Map(servers.map(server => [server.address, server])),
    type === TopologyType.Sharded || type === TopologyType.Single ? null : 'rs',
    null,
    null,
    MIN_SECONDARY_WRITE_WIRE_VERSION,
    { localThresholdMS }
  );
}

function addresses(servers: ServerDescription[]): string[] {
  return servers.map(server => server.address);
}

describe('serverSelector', function () {
  describe('option parsing', function () {
    const serverSelector: ServerSelectorFunction = servers => servers;

    it('accepts a function passed to MongoClient', function () {
      const options = parseOptions('mongodb://localhost:27017', { serverSelector });
      expect(options.serverSelector).to.equal(serverSelector);

      const client = new MongoClient('mongodb://localhost:27017', { serverSelector });
      expect(client.options.serverSelector).to.equal(serverSelector);
    });

    it('leaves the option unset when it is omitted or null', function () {
      expect(parseOptions('mongodb://localhost:27017').serverSelector).to.equal(undefined);
      expect(
        parseOptions('mongodb://localhost:27017', { serverSelector: null as never }).serverSelector
      ).to.equal(undefined);
    });

    it('rejects a non-function', function () {
      expect(() =>
        parseOptions('mongodb://localhost:27017', { serverSelector: 'host' as never })
      ).to.throw(MongoParseError, /serverSelector must be a function/);
    });

    it('rejects the option in a connection string', function () {
      expect(() => parseOptions('mongodb://localhost:27017/?serverSelector=host')).to.throw(
        MongoParseError,
        /URI cannot contain `serverSelector`, it can only be passed to the client/
      );
    });
  });

  describe('selection', function () {
    const primary = member('127.0.0.1:27017', 'primary', 10);
    const secondary = member('127.0.0.1:27018', 'secondary', 20);
    const slowSecondary = member('127.0.0.1:27019', 'secondary', 100);

    it('sees every eligible server before the latency window', function () {
      let seen: string[] = [];
      const selector = applyUserServerSelector(readPreferenceNearest(), servers => {
        seen = addresses(servers);
        return servers;
      });

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary, slowSecondary]),
        [primary, secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(seen).to.deep.equal([primary.address, secondary.address, slowSecondary.address]);
      expect(addresses(selected)).to.deep.equal([primary.address, secondary.address]);
    });

    it('keeps a single slower member outside the latency window', function () {
      const selector = applyUserServerSelector(readPreferenceNearest(), servers =>
        servers.filter(server => server.address === slowSecondary.address)
      );

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary, slowSecondary]),
        [primary, secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(addresses(selected)).to.deep.equal([slowSecondary.address]);
    });

    it('passes only the primary to the filter for a primary read', function () {
      let seen: string[] = [];
      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), servers => {
        seen = addresses(servers);
        return servers;
      });

      selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, slowSecondary]),
        [primary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(seen).to.deep.equal([primary.address]);
    });

    it('does not call the filter when no server is eligible', function () {
      let calls = 0;
      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), servers => {
        calls += 1;
        return servers;
      });

      const selected = selector(
        description(TopologyType.ReplicaSetNoPrimary, [secondary, slowSecondary]),
        [secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(calls).to.equal(0);
      expect(selected).to.deep.equal([]);
    });

    it('can narrow secondary writes to one secondary', function () {
      const selector = applyUserServerSelector(
        secondaryWritableServerSelector(MIN_SECONDARY_WRITE_WIRE_VERSION, ReadPreference.secondary),
        servers => servers.filter(server => server.address === slowSecondary.address)
      );

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary, slowSecondary]),
        [primary, secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(addresses(selected)).to.deep.equal([slowSecondary.address]);
    });

    it('ignores the filter for a single topology', function () {
      let calls = 0;
      const standalone = member('127.0.0.1:27021', 'standalone', 10);
      const slow = member('127.0.0.1:27022', 'standalone', 100);
      const selector = applyUserServerSelector(readPreferenceNearest(), () => {
        calls += 1;
        return [];
      });

      const selected = selector(
        description(TopologyType.Single, [standalone, slow]),
        [standalone, slow],
        new DeprioritizedServers()
      );

      expect(calls).to.equal(0);
      expect(addresses(selected)).to.deep.equal([standalone.address]);
    });

    it('ignores the filter for a load-balanced topology', function () {
      let calls = 0;
      const balancer = member('127.0.0.1:27020', 'loadBalancer', 10);
      const selector = applyUserServerSelector(readPreferenceNearest(), () => {
        calls += 1;
        return [];
      });

      const selected = selector(
        description(TopologyType.LoadBalanced, [balancer]),
        [balancer],
        new DeprioritizedServers()
      );

      expect(calls).to.equal(0);
      expect(selected).to.deep.equal([balancer]);
    });

    it('does not move an operation pinned to a server', function () {
      let calls = 0;
      const selector = applyUserServerSelector(sameServerSelector(slowSecondary), () => {
        calls += 1;
        return [primary];
      });

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, slowSecondary]),
        [primary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(calls).to.equal(0);
      expect(addresses(selected)).to.deep.equal([slowSecondary.address]);
    });

    it('rejects a return value that is not a list of server descriptions', function () {
      const selector = applyUserServerSelector(readPreferenceNearest(), () => 'nope' as never);
      expect(() =>
        selector(
          description(TopologyType.ReplicaSetWithPrimary, [primary]),
          [primary],
          new DeprioritizedServers()
        )
      ).to.throw(MongoInvalidArgumentError, /array of server descriptions/);
    });

    it('gives the filter copies and selects the eligible instances', function () {
      let seen: ServerDescription[] = [];
      const selector = applyUserServerSelector(readPreferenceNearest(), servers => {
        seen = servers;
        servers[0].roundTripTime = 999;
        servers[0].tags.nodeType = 'analytics';
        servers[0].hosts.push('evil:27017');
        return servers;
      });

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary, slowSecondary]),
        [primary, secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(seen[0]).to.not.equal(primary);
      expect(primary.roundTripTime).to.equal(10);
      expect(primary.tags).to.deep.equal({});
      expect(primary.hosts).to.deep.equal([]);
      expect(selected[0]).to.equal(primary);
      expect(selected[1]).to.equal(secondary);
    });

    it('keeps nested SDAM fields isolated from the filter', function () {
      const electionId = new ObjectId();
      const processId = new ObjectId();
      const counter = Long.fromNumber(4);
      const clusterTime = new Timestamp({ t: 2, i: 3 });
      const keyId = Long.fromNumber(1);
      const hash = new Binary(Uint8Array.from([1, 2, 3]));
      // One topologyVersion object is stored on the description, on the server
      // error, and inside that error's response document. Same for electionId
      // and $clusterTime: the description keeps the hello's objects.
      const topologyVersion = { processId, counter };
      const error = new MongoServerError({
        message: 'ping failed',
        ok: 0,
        topologyVersion
      });
      const described = new ServerDescription(
        '127.0.0.1:27017',
        {
          setName: 'rs',
          isWritablePrimary: true,
          ok: 1,
          electionId,
          $clusterTime: { clusterTime, signature: { hash, keyId } }
        },
        { roundTripTime: 10, error }
      );
      expect(described.electionId).to.equal(electionId);
      expect(described.topologyVersion).to.equal(error.topologyVersion);
      expect(error.errorResponse.topologyVersion).to.equal(topologyVersion);
      expect(described.$clusterTime?.clusterTime).to.equal(clusterTime);
      expect(described.$clusterTime?.signature?.hash).to.equal(hash);

      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), servers => {
        const seen = servers[0];
        if (
          seen.electionId == null ||
          seen.topologyVersion == null ||
          seen.$clusterTime?.signature == null ||
          !(seen.error instanceof MongoServerError) ||
          seen.error.topologyVersion == null ||
          seen.error.errorResponse.topologyVersion == null
        ) {
          throw new Error('expected nested SDAM fields on the copy');
        }
        // Edit the copy in place. Replacing the property would hide a shared
        // Long, ObjectId, Timestamp, or Binary.
        const idBytes = Uint8Array.from(seen.electionId.id);
        idBytes[0] ^= 0xff;
        seen.electionId.id = idBytes;
        seen.topologyVersion.processId = new ObjectId();
        seen.topologyVersion.counter.low = 1;
        seen.$clusterTime.clusterTime.low = 1;
        seen.$clusterTime.signature.hash.value()[0] = 9;
        seen.$clusterTime.signature.keyId.low = 9;
        seen.error.message = 'changed';
        seen.error.topologyVersion.counter.low = 7;
        seen.error.errorResponse.message = 'changed response';
        seen.error.errorResponse.topologyVersion.counter.low = 8;
        return servers;
      });

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [described]),
        [described],
        new DeprioritizedServers()
      );

      expect(selected[0]).to.equal(described);
      expect(described.electionId).to.equal(electionId);
      expect(described.topologyVersion?.processId).to.equal(processId);
      expect(described.topologyVersion?.counter.toNumber()).to.equal(4);
      expect(described.$clusterTime?.clusterTime.t).to.equal(2);
      expect(described.$clusterTime?.clusterTime.i).to.equal(3);
      expect(Array.from(described.$clusterTime?.signature?.hash.value() ?? [])).to.deep.equal([
        1, 2, 3
      ]);
      expect(described.$clusterTime?.signature?.keyId.toNumber()).to.equal(1);
      expect(described.error).to.be.instanceOf(MongoServerError);
      if (!(described.error instanceof MongoServerError)) {
        throw new Error('expected the original MongoServerError');
      }
      expect(described.error.message).to.equal('ping failed');
      expect(described.error.topologyVersion?.counter.toNumber()).to.equal(4);
      expect(described.error.errorResponse.message).to.equal('ping failed');
      expect(described.error.errorResponse.topologyVersion?.counter.toNumber()).to.equal(4);
    });

    it('keeps a network error class and isolates its topologyVersion', function () {
      const processId = new ObjectId();
      const counter = Long.fromNumber(4);
      const topologyVersion = { processId, counter };
      const error = new MongoNetworkError('connection closed', { beforeHandshake: false });
      error.topologyVersion = topologyVersion;
      error.addErrorLabel('ResetPool');
      const described = new ServerDescription(
        '127.0.0.1:27017',
        { setName: 'rs', isWritablePrimary: true, ok: 1 },
        { roundTripTime: 10, error }
      );

      let seenError: MongoNetworkError | undefined;
      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), servers => {
        const seen = servers[0].error;
        if (!(seen instanceof MongoNetworkError) || seen.topologyVersion == null) {
          throw new Error('expected a MongoNetworkError copy');
        }
        seenError = seen;
        seen.message = 'changed';
        seen.topologyVersion.counter.low = 1;
        seen.topologyVersion.processId = new ObjectId();
        return servers;
      });

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [described]),
        [described],
        new DeprioritizedServers()
      );

      expect(selected[0]).to.equal(described);
      expect(seenError).to.be.instanceOf(MongoNetworkError);
      expect(seenError?.beforeHandshake).to.equal(false);
      expect(seenError?.hasErrorLabel('ResetPool')).to.equal(true);
      expect(error.message).to.equal('connection closed');
      expect(error.topologyVersion?.processId).to.equal(processId);
      expect(error.topologyVersion?.counter.toNumber()).to.equal(4);
      expect(described.topologyVersion?.counter.toNumber()).to.equal(4);
    });

    it('rejects a server that was not eligible', function () {
      const other = member('127.0.0.1:27999', 'secondary', 1);
      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), () => [
        other
      ]);

      expect(() =>
        selector(
          description(TopologyType.ReplicaSetWithPrimary, [primary, other]),
          [primary, other],
          new DeprioritizedServers()
        )
      ).to.throw(MongoInvalidArgumentError, /127\.0\.0\.1:27999, which is not eligible/);
    });

    it('rejects a secondary returned for a write', function () {
      const selector = applyUserServerSelector(writableServerSelector(), () => [secondary]);

      expect(() =>
        selector(
          description(TopologyType.ReplicaSetWithPrimary, [primary, secondary]),
          [primary, secondary],
          new DeprioritizedServers()
        )
      ).to.throw(MongoInvalidArgumentError, /not eligible/);
    });

    it('selects the eligible server when the filter returns another object with its address', function () {
      const selector = applyUserServerSelector(readPreferenceNearest(), () => [
        { address: slowSecondary.address, roundTripTime: 0 } as ServerDescription
      ]);

      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary, slowSecondary]),
        [primary, secondary, slowSecondary],
        new DeprioritizedServers()
      );

      expect(selected).to.have.lengthOf(1);
      expect(selected[0]).to.equal(slowSecondary);
    });

    it('returns an empty list when the filter returns an empty list', function () {
      const selector = applyUserServerSelector(readPreferenceNearest(), () => []);
      const selected = selector(
        description(TopologyType.ReplicaSetWithPrimary, [primary, secondary]),
        [primary, secondary],
        new DeprioritizedServers()
      );
      expect(selected).to.deep.equal([]);
    });

    it('applies the latency window to mongos after the filter', function () {
      const fast = member('127.0.0.1:27017', 'mongos', 10);
      const slow = member('127.0.0.1:27018', 'mongos', 100);
      let seen: string[] = [];
      const selector = applyUserServerSelector(readPreference(ReadPreference.primary), servers => {
        seen = addresses(servers);
        return [slow];
      });

      const selected = selector(
        description(TopologyType.Sharded, [fast, slow]),
        [fast, slow],
        new DeprioritizedServers()
      );

      expect(seen).to.deep.equal([fast.address, slow.address]);
      expect(addresses(selected)).to.deep.equal([slow.address]);
    });
  });

  describe('Topology#selectServer', function () {
    const primary = member('127.0.0.1:27017', 'primary', 10);
    const slowSecondary = member('127.0.0.1:27019', 'secondary', 100);

    it('uses the serverSelector parsed onto the client options', async function () {
      const parsed = parseOptions('mongodb://127.0.0.1:27017,127.0.0.1:27019', {
        serverSelectionTimeoutMS: 500,
        localThresholdMS: LOCAL_THRESHOLD_MS,
        serverSelector: servers =>
          servers.filter(server => server.address === slowSecondary.address)
      });
      const topology = topologyWithPlaceholderClient(
        ['127.0.0.1:27017', '127.0.0.1:27019'],
        parsed
      );
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary, slowSecondary]);

      const selected = await topology.selectServer(ReadPreference.nearest, {
        operationName: 'find',
        deprioritizedServers: new DeprioritizedServers()
      });

      expect(selected).to.equal(topology.s.servers.get(slowSecondary.address));
    });

    it('keeps getMore on its pinned server when serverSelector would reject it', async function () {
      const topology = topologyWithPlaceholderClient(['127.0.0.1:27017', '127.0.0.1:27019'], {
        serverSelectionTimeoutMS: 500,
        serverSelector() {
          throw new Error('serverSelector must not run for a pinned operation');
        }
      });
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary, slowSecondary]);

      const selected = await topology.selectServer(sameServerSelector(slowSecondary), {
        operationName: 'getMore',
        deprioritizedServers: new DeprioritizedServers()
      });

      expect(selected).to.equal(topology.s.servers.get(slowSecondary.address));
    });

    it('fails selection when the filter throws', async function () {
      const topology = topologyWithPlaceholderClient('127.0.0.1:27017', {
        serverSelectionTimeoutMS: 500,
        serverSelector() {
          throw new Error('selector failed');
        }
      });
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary]);

      const error = await topology
        .selectServer(ReadPreference.primary, {
          operationName: 'find',
          deprioritizedServers: new DeprioritizedServers()
        })
        .catch(caught => caught);

      expect(error).to.be.instanceOf(Error);
      expect(error.message).to.equal('selector failed');
    });

    it('does not call the filter for the connect handshake', async function () {
      let calls = 0;
      const topology = topologyWithPlaceholderClient('127.0.0.1:27017', {
        serverSelectionTimeoutMS: 500,
        serverSelector() {
          calls += 1;
          return [];
        }
      });
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary]);

      const selected = await topology.selectServer(ReadPreference.primary, {
        operationName: 'handshake',
        deprioritizedServers: new DeprioritizedServers()
      });

      expect(calls).to.equal(0);
      expect(selected).to.equal(topology.s.servers.get(primary.address));
    });

    it('does not call the filter for endSessions', async function () {
      let calls = 0;
      const topology = topologyWithPlaceholderClient('127.0.0.1:27017', {
        serverSelectionTimeoutMS: 500,
        serverSelector() {
          calls += 1;
          throw new Error('serverSelector must not run while ending sessions');
        }
      });
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary]);

      const selected = await topology.selectServer(ReadPreference.primaryPreferred, {
        operationName: 'endSessions',
        deprioritizedServers: new DeprioritizedServers()
      });

      expect(calls).to.equal(0);
      expect(selected).to.equal(topology.s.servers.get(primary.address));
    });

    it('times out when the filter returns no servers', async function () {
      const topology = topologyWithPlaceholderClient('127.0.0.1:27017', {
        serverSelectionTimeoutMS: 50,
        serverSelector() {
          return [];
        }
      });
      install(topology, TopologyType.ReplicaSetWithPrimary, [primary]);

      const error = await topology
        .selectServer(ReadPreference.primary, {
          operationName: 'find',
          deprioritizedServers: new DeprioritizedServers()
        })
        .catch(caught => caught);

      expect(error).to.be.instanceOf(MongoServerSelectionError);
    });
  });
});

function readPreference(preference: ReadPreference) {
  return readPreferenceServerSelector(preference);
}

function readPreferenceNearest() {
  return readPreference(ReadPreference.nearest);
}

function install(
  topology: ReturnType<typeof topologyWithPlaceholderClient>,
  type: TopologyType,
  servers: ServerDescription[]
) {
  topology.s.state = 'connected';
  topology.s.description = description(type, servers);
  topology.s.servers.clear();
  for (const server of servers) {
    topology.s.servers.set(server.address, {
      address: server.address,
      requestCheck() {
        return Promise.resolve();
      }
    } as never);
  }
}
