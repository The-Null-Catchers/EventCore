import {test} from 'node:test';
import assert from 'node:assert/strict';
import {EventCore,EventCoreError} from '../dist/index.js';
const client=new EventCore({baseUrl:'https://eventcore.example',apiKey:'test'});
test('publish sends envelope and never retries ambiguous failures',async()=>{let calls=0;const original=globalThis.fetch;globalThis.fetch=async(url,options)=>{calls++;assert.equal(url,'https://eventcore.example/v1/topics/orders/events');assert.equal(options.headers.Authorization,'Bearer test');assert.equal(JSON.parse(options.body).type,'order');throw new Error('connection lost after append')};try{await assert.rejects(client.publish('orders',{type:'order',data:{}}));assert.equal(calls,1)}finally{globalThis.fetch=original}});
test('typed errors retain status',async()=>{const original=globalThis.fetch;globalThis.fetch=async()=>new Response(JSON.stringify({error:'scope denied'}),{status:403});try{await assert.rejects(client.request('GET','/v1/topics'),e=>e instanceof EventCoreError&&e.status===403)}finally{globalThis.fetch=original}});
test('batch acknowledgment uses server lease and generation',async()=>{const original=globalThis.fetch;let ack;globalThis.fetch=async(url,options)=>{if(url.endsWith('/pull'))return Response.json([{partition:2,token:'lease',epoch:7,events:[{offset:9}]}]);ack=JSON.parse(options.body);return Response.json({ok:true})};try{const [d]=await client.pull('orders','billing','member',7);await d.ack();assert.deepEqual(ack,{member:'member',epoch:7,partition:2,token:'lease'})}finally{globalThis.fetch=original}});
test('partial commit carries next offset and surfaces fencing without retry',async()=>{
 const original=globalThis.fetch;let commits=0;
 globalThis.fetch=async(url,options)=>{
  if(url.endsWith('/pull'))return Response.json([{partition:2,token:'lease',epoch:7,events:[{offset:9},{offset:10}]}]);
  assert.ok(url.endsWith('/commit'));commits++;
  assert.deepEqual(JSON.parse(options.body),{member:'member',epoch:7,partition:2,token:'lease',next_offset:10});
  return Response.json({error:'stale lease'},{status:409});
 };
 try{const [d]=await client.pull('orders','billing','member',7);await assert.rejects(d.commit(10),e=>e instanceof EventCoreError&&e.status===409);assert.equal(commits,1)}finally{globalThis.fetch=original}
});

test('producer idempotency keys are explicit and response preserves receipt',async()=>{
 const original=globalThis.fetch;let calls=0;
 globalThis.fetch=async(url,options)=>{calls++;assert.equal(JSON.parse(options.body).idempotency_key,'req-123');return Response.json({id:'same',deduplicated:true})};
 try{const result=await client.publish('orders',{type:'created',data:{},idempotency_key:'req-123'});assert.equal(result.id,'same');assert.equal(result.deduplicated,true);assert.equal(calls,1)}finally{globalThis.fetch=original}
});
