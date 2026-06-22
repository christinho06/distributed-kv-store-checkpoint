const { DistributedKVStore } = require('./distributed-kv-store');

function section(n, titre) {
  console.log(`\n${'='.repeat(54)}`);
  console.log(`  Etape ${n} — ${titre}`);
  console.log('='.repeat(54));
}

const store = new DistributedKVStore();

// ── Etape 1 : Creation du cluster ────────────────────────────
section(1, 'Creation du cluster (3 noeuds)');
['NodeA', 'NodeB', 'NodeC'].forEach(id => store.addNode(id));
console.log('  Noeuds actifs : NodeA, NodeB, NodeC');
store.status();

// ── Etape 2 : Insertion des donnees experimentales ───────────
section(2, 'Insertion des donnees');
const users = [
  ['user:101', { name: 'Alice'   }],
  ['user:102', { name: 'Bob'     }],
  ['user:103', { name: 'Charlie' }],
  ['user:104', { name: 'Diana'   }],
  ['user:105', { name: 'Eve'     }],
  ['user:106', { name: 'Frank'   }],
];

users.forEach(([key, value]) => {
  const r = store.set(key, JSON.stringify(value));
  console.log(`  SET ${key} -> noeud: ${r.node} | succes: ${r.success}`);
});
store.status();

// ── Etape 3 : Cache miss puis cache hit ──────────────────────
section(3, 'Lecture : cache miss puis cache hit');
const r1 = store.get('user:101');
console.log(`  1er GET user:101 -> source: ${r1.source} | valeur: ${r1.value} | noeud: ${r1.node}`);
const r2 = store.get('user:101');
console.log(`  2e  GET user:101 -> source: ${r2.source} | valeur: ${r2.value} | noeud: ${r2.node}`);
console.log('  => La 2e lecture vient du cache sans toucher au store.');

// ── Etape 4 : Ajout d'un noeud (redistribution minimale) ────
section(4, 'Ajout de NodeD — redistribution minimale');
console.log('  Seules les cles desormais assignees a NodeD sont deplacees.');
store.addNode('NodeD');
store.status();

// ── Etape 5 : Suppression d'un noeud ─────────────────────────
section(5, 'Suppression de NodeB — redistribution vers successeur');
console.log('  Les cles de NodeB migrent vers leurs successeurs dans l\'anneau.');
store.removeNode('NodeB');
store.status();

// ── Etape 6 : Verification de l'integrite ───────────────────
section(6, 'Integrite des donnees apres suppression de NodeB');
users.forEach(([key]) => {
  const r = store.get(key);
  const info = r.error
    ? `ERREUR: ${r.error}`
    : `valeur: ${r.value} | source: ${r.source} | noeud: ${r.node}`;
  console.log(`  GET ${key} -> ${info}`);
});
console.log('  => Toutes les donnees restent accessibles (aucune perte).');

// ── Etape 7 : Simulation de panne ───────────────────────────
section(7, 'Simulation de panne de NodeA');
store.failNode('NodeA');
store.status();

let pannes = 0;
users.forEach(([key]) => {
  const r = store.get(key);
  if (r.error) {
    console.log(`  GET ${key} -> INDISPONIBLE (${r.error})`);
    pannes++;
  } else {
    console.log(`  GET ${key} -> valeur: ${r.value} | noeud: ${r.node}`);
  }
});
console.log(`  => ${pannes} cle(s) inaccessible(s) tant que NodeA est hors ligne.`);

// ── Etape 8 : Recuperation du noeud ─────────────────────────
section(8, 'Recuperation de NodeA');
store.recoverNode('NodeA');
store.status();

console.log('  Verification apres recuperation :');
users.forEach(([key]) => {
  const r = store.get(key);
  const info = r.error
    ? `ERREUR: ${r.error}`
    : `valeur: ${r.value} | noeud: ${r.node}`;
  console.log(`  GET ${key} -> ${info}`);
});
console.log('  => Toutes les donnees sont de nouveau accessibles.');
