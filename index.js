const crypto=require('crypto');

class ConsistentHash{
  constructor(replicas=150){this.replicas=replicas;this.ring=new Map();this.sortedKeys=[];}
  addNode(node){
    for(let i=0;i<this.replicas;i++){
      const key=this._hash(`${node}:${i}`);
      this.ring.set(key,node);this.sortedKeys.push(key);
    }
    this.sortedKeys.sort((a,b)=>a-b);
  }
  removeNode(node){
    for(let i=0;i<this.replicas;i++){
      const key=this._hash(`${node}:${i}`);
      this.ring.delete(key);
      this.sortedKeys=this.sortedKeys.filter(k=>k!==key);
    }
  }
  getNode(key){
    if(!this.ring.size)return null;
    const h=this._hash(key);
    const idx=this.sortedKeys.findIndex(k=>k>=h);
    const chosen=idx===-1?this.sortedKeys[0]:this.sortedKeys[idx];
    return this.ring.get(chosen);
  }
  _hash(str){let h=0;for(const c of str){h=(h<<5)-h+c.charCodeAt(0);h|=0;}return Math.abs(h);}
}

class LRUCache{
  constructor(capacity=100){this.capacity=capacity;this.cache=new Map();}
  get(key){if(!this.cache.has(key))return null;const v=this.cache.get(key);this.cache.delete(key);this.cache.set(key,v);return v;}
  put(key,value){if(this.cache.has(key))this.cache.delete(key);else if(this.cache.size>=this.capacity)this.cache.delete(this.cache.keys().next().value);this.cache.set(key,value);}
}

class KVNode{
  constructor(id){this.id=id;this.store=new Map();this.cache=new LRUCache(50);}
  set(key,value){this.store.set(key,value);this.cache.put(key,value);return true;}
  get(key){const c=this.cache.get(key);if(c!==null)return c;const v=this.store.get(key)||null;if(v)this.cache.put(key,v);return v;}
  delete(key){this.cache.cache.delete(key);return this.store.delete(key);}
}

class DistributedKVStore{
  constructor(){this.ring=new ConsistentHash();this.nodes=new Map();}
  addNode(id){const node=new KVNode(id);this.nodes.set(id,node);this.ring.addNode(id);return node;}
  set(key,value){const nodeId=this.ring.getNode(key);const node=this.nodes.get(nodeId);return node?node.set(key,value):false;}
  get(key){const nodeId=this.ring.getNode(key);const node=this.nodes.get(nodeId);return node?node.get(key):null;}
  delete(key){const nodeId=this.ring.getNode(key);const node=this.nodes.get(nodeId);return node?node.delete(key):false;}
}

module.exports={DistributedKVStore,ConsistentHash,LRUCache};
