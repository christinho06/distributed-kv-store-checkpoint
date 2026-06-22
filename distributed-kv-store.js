const crypto = require('crypto');

// ── Hachage coherent ──────────────────────────────────────────────────────────
class ConsistentHash {
  constructor(replicas = 150) {
    this.replicas = replicas;
    this.ring = new Map();
    this.sortedKeys = [];
  }

  addNode(node) {
    for (let i = 0; i < this.replicas; i++) {
      const key = this._hash(`${node}:${i}`);
      this.ring.set(key, node);
      this.sortedKeys.push(key);
    }
    this.sortedKeys.sort((a, b) => a - b);
  }

  removeNode(node) {
    for (let i = 0; i < this.replicas; i++) {
      const key = this._hash(`${node}:${i}`);
      this.ring.delete(key);
      this.sortedKeys = this.sortedKeys.filter(k => k !== key);
    }
  }

  getNode(key) {
    if (!this.ring.size) return null;
    const h = this._hash(key);
    const idx = this.sortedKeys.findIndex(k => k >= h);
    const chosen = idx === -1 ? this.sortedKeys[0] : this.sortedKeys[idx];
    return this.ring.get(chosen);
  }

  // crypto.createHash remplace le hash maison : meilleure distribution
  _hash(str) {
    return parseInt(crypto.createHash('md5').update(str).digest('hex').slice(0, 8), 16);
  }
}

// ── Cache LRU ────────────────────────────────────────────────────────────────
class LRUCache {
  constructor(capacity = 100) {
    this.capacity = capacity;
    this.cache = new Map();
  }

  get(key) {
    if (!this.cache.has(key)) return null;
    const v = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, v); // deplace en fin = "recemment utilise"
    return v;
  }

  put(key, value) {
    if (this.cache.has(key)) this.cache.delete(key);
    else if (this.cache.size >= this.capacity)
      this.cache.delete(this.cache.keys().next().value); // expulse le plus ancien
    this.cache.set(key, value);
  }

  delete(key) {
    this.cache.delete(key);
  }

  get size() {
    return this.cache.size;
  }
}

// ── Noeud KV ─────────────────────────────────────────────────────────────────
class KVNode {
  constructor(id) {
    this.id    = id;
    this.store = new Map();
    this.cache = new LRUCache(50);
    this.alive = true;
  }

  set(key, value) {
    if (!this.alive) throw new Error(`Noeud ${this.id} hors ligne`);
    this.store.set(key, value);
    this.cache.put(key, value);
    return true;
  }

  // Retourne { value, source } pour distinguer cache hit et store hit
  get(key) {
    if (!this.alive) throw new Error(`Noeud ${this.id} hors ligne`);
    const cached = this.cache.get(key);
    if (cached !== null) return { value: cached, source: 'cache' };
    const stored = this.store.get(key) ?? null;
    if (stored !== null) this.cache.put(key, stored);
    return { value: stored, source: 'store' };
  }

  delete(key) {
    if (!this.alive) throw new Error(`Noeud ${this.id} hors ligne`);
    this.cache.delete(key);
    return this.store.delete(key);
  }

  // Panne : le noeud reste dans l'anneau mais refuse les requetes
  fail()    { this.alive = false; }
  recover() { this.alive = true;  }
}

// ── Magasin distribue ────────────────────────────────────────────────────────
class DistributedKVStore {
  constructor() {
    this.ring  = new ConsistentHash();
    this.nodes = new Map();
  }

  // Ajoute un noeud et migre vers lui les cles qui lui reviennent
  addNode(id) {
    const node = new KVNode(id);
    this.nodes.set(id, node);
    this.ring.addNode(id);

    let migrated = 0;
    this.nodes.forEach((existing, existingId) => {
      if (existingId === id) return;
      const toMove = [];
      existing.store.forEach((value, key) => {
        if (this.ring.getNode(key) === id) toMove.push({ key, value });
      });
      toMove.forEach(({ key, value }) => {
        node.store.set(key, value);
        node.cache.put(key, value);
        existing.store.delete(key);
        existing.cache.delete(key);
        migrated++;
      });
    });

    if (migrated > 0)
      console.log(`  [Migration] ${migrated} cle(s) transferee(s) vers ${id}`);
    return node;
  }

  // Retire un noeud et redistribue ses cles vers leurs successeurs
  removeNode(id) {
    const node = this.nodes.get(id);
    if (!node) return false;

    const snapshot = new Map(node.store); // copie avant suppression
    this.ring.removeNode(id);
    this.nodes.delete(id);

    let migrated = 0;
    snapshot.forEach((value, key) => {
      const successorId = this.ring.getNode(key);
      const successor   = this.nodes.get(successorId);
      if (successor) {
        successor.store.set(key, value);
        successor.cache.put(key, value);
        migrated++;
      }
    });

    console.log(`  [Migration] ${migrated} cle(s) redistribuee(s) depuis ${id}`);
    return true;
  }

  // Simule une panne : noeud garde ses donnees mais refuse les requetes
  failNode(id) {
    const node = this.nodes.get(id);
    if (!node) return false;
    node.fail();
    console.log(`  [Panne] Noeud ${id} hors ligne`);
    return true;
  }

  recoverNode(id) {
    const node = this.nodes.get(id);
    if (!node) return false;
    node.recover();
    console.log(`  [Recuperation] Noeud ${id} de nouveau en ligne`);
    return true;
  }

  // API transparente : l'utilisateur ne sait pas sur quel noeud la cle est stockee
  set(key, value) {
    const nodeId = this.ring.getNode(key);
    const node   = this.nodes.get(nodeId);
    if (!node) return { success: false, error: 'Aucun noeud disponible' };
    try {
      node.set(key, value);
      return { success: true, node: nodeId };
    } catch (e) {
      return { success: false, error: e.message, node: nodeId };
    }
  }

  get(key) {
    const nodeId = this.ring.getNode(key);
    const node   = this.nodes.get(nodeId);
    if (!node) return { value: null, error: 'Aucun noeud disponible' };
    try {
      const { value, source } = node.get(key);
      return { value, source, node: nodeId };
    } catch (e) {
      return { value: null, error: e.message, node: nodeId };
    }
  }

  delete(key) {
    const nodeId = this.ring.getNode(key);
    const node   = this.nodes.get(nodeId);
    if (!node) return false;
    try   { return node.delete(key); }
    catch (e) { console.log(`  [Erreur] ${e.message}`); return false; }
  }

  status() {
    console.log('\n  +--- Etat du cluster -----------------------------------+');
    this.nodes.forEach((node, id) => {
      const etat = node.alive ? '[EN LIGNE]  ' : '[HORS LIGNE]';
      console.log(`  |  ${id}: ${etat} | Cles: ${node.store.size} | Cache: ${node.cache.size}`);
    });
    console.log('  +-------------------------------------------------------+\n');
  }
}

module.exports = { DistributedKVStore, ConsistentHash, LRUCache };
