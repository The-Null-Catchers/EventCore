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
test('replay scan encodes filters and copy carries fixed range and run ID',async()=>{
 const original=globalThis.fetch;let calls=0;
 globalThis.fetch=async(url,options)=>{calls++;if(options.method==='GET'){const q=new URL(url).searchParams;assert.equal(q.get('offset'),'0');assert.equal(q.get('end_offset'),'50');assert.equal(q.get('type'),'order.created');return Response.json({events:[],next_offset:20,end_offset:50,scanned:20,done:false})};const body=JSON.parse(options.body);assert.equal(body.replay_id,'run-1');assert.equal(body.end_offset,50);assert.equal(body.confirm,true);return Response.json({receipts:[],next_offset:20,end_offset:50,done:false,scanned:20})};
 try{const page=await client.scan('orders',{partition:1,offset:0,end_offset:50,type:'order.created'});assert.equal(page.next_offset,20);await client.replay('orders',{target:'archive',replay_id:'run-1',partition:1,offset:0,end_offset:50});assert.equal(calls,2)}finally{globalThis.fetch=original}
});
test('replay failures expose durable prefix without retrying mutation',async()=>{
 const original=globalThis.fetch;let calls=0;const progress={error:'disk pressure',receipts:[{source_offset:5}],next_offset:6,done:false};
 globalThis.fetch=async()=>{calls++;return Response.json(progress,{status:503})};
 try{await assert.rejects(client.replay('orders',{target:'archive',replay_id:'run',partition:0,offset:5,end_offset:20}),e=>e instanceof EventCoreError&&e.status===503&&e.details.next_offset===6);assert.equal(calls,1)}finally{globalThis.fetch=original}
});
test('DLQ resolution does not retry an ambiguous mutation',async()=>{
 const original=globalThis.fetch;let calls=0;
 globalThis.fetch=async(url,options)=>{calls++;assert.ok(url.endsWith('/orders.DLQ/dead-letters/retry'));assert.deepEqual(JSON.parse(options.body),{partition:0,offset:9,confirm:true});throw new Error('connection lost')};
 try{await assert.rejects(client.resolveDeadLetter('orders.DLQ',0,9,'retry'));assert.equal(calls,1)}finally{globalThis.fetch=original}
});
