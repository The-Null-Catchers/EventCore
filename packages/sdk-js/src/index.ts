export interface EventInput<T=unknown> { type: string; key?: string; headers?: Record<string,string>; data: T }
export interface Event<T=unknown> extends EventInput<T> { id: string; topic: string; partition: number; offset: number; timestamp: string }
export interface Batch { partition: number; token: string; epoch: number; events: Event[] }
export interface Delivery extends Batch { ack(): Promise<void>; nack(): Promise<void> }
export interface Group { epoch: number; offsets: Record<string,number>; lag: Record<string,number>; members: {id:string;partitions:number[]}[] }
export class EventCoreError extends Error { constructor(readonly status:number, message:string){super(message)} }
const sleep=(ms:number)=>new Promise<void>(resolve=>setTimeout(resolve,ms));
export class EventCore {
  private baseUrl:string;
  constructor(private options:{baseUrl:string;apiKey:string;timeoutMs?:number}){this.baseUrl=options.baseUrl.replace(/\/$/,'')}
  async request<T>(method:string,path:string,body?:unknown):Promise<T>{
    const attempts=method==='GET'?3:1;
    for(let attempt=0;attempt<attempts;attempt++){
      try{
        const response=await fetch(this.baseUrl+path,{method,headers:{Authorization:`Bearer ${this.options.apiKey}`,'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(this.options.timeoutMs??10000)});
        const data=await response.json();
        if(!response.ok)throw new EventCoreError(response.status,data.error??`HTTP ${response.status}`);
        return data as T;
      }catch(error){
        const transient=!(error instanceof EventCoreError)||[429,502,503,504].includes(error.status);
        if(attempt+1===attempts||!transient)throw error;
        await sleep(200*2**attempt);
      }
    }
    throw new Error('unreachable');
  }
  topicPath(topic:string){return `/v1/topics/${encodeURIComponent(topic)}`}
  groupPath(topic:string,group:string){return `${this.topicPath(topic)}/groups/${encodeURIComponent(group)}`}
  publish<T>(topic:string,event:EventInput<T>):Promise<Event<T>>{return this.request('POST',`${this.topicPath(topic)}/events`,event)}
  async publishBatch(topic:string,events:EventInput[]):Promise<({event:Event}|{error:string})[]>{const response=await this.request<{results:({event:Event}|{error:string})[]}>('POST',`${this.topicPath(topic)}/events/batch`,{events});return response.results}
  join(topic:string,group:string,member:string):Promise<Group>{return this.request('POST',`${this.groupPath(topic,group)}/join`,{member})}
  async pull(topic:string,group:string,member:string,epoch:number,limit=100):Promise<Delivery[]>{
    const path=this.groupPath(topic,group);
    const batches=await this.request<Batch[]>('POST',`${path}/pull`,{member,epoch,limit});
    return batches.map(batch=>({...batch,ack:async()=>{await this.request('POST',`${path}/ack`,{member,epoch:batch.epoch,partition:batch.partition,token:batch.token})},nack:async()=>{await this.request('POST',`${path}/nack`,{member,epoch:batch.epoch,partition:batch.partition,token:batch.token})}}));
  }
  async subscribe(topic:string,options:{group:string;member:string;signal:AbortSignal},callback:(delivery:Delivery)=>Promise<void>):Promise<void>{
    let epoch=(await this.join(topic,options.group,options.member)).epoch;
    let delay=200;
    try{
      while(!options.signal.aborted){
        let batches:Delivery[];
        try{batches=await this.pull(topic,options.group,options.member,epoch);delay=200}
        catch(error){
          if(error instanceof EventCoreError&&error.status===409){epoch=(await this.join(topic,options.group,options.member)).epoch;continue}
          if(error instanceof EventCoreError&&![429,502,503,504].includes(error.status))throw error;
          await sleep(delay);delay=Math.min(delay*2,5000);continue;
        }
        // Callback exceptions propagate; no implicit commit after processing failures.
        for(const batch of batches){if(options.signal.aborted)break;await callback(batch)}
        if(batches.length===0)await sleep(200);
      }
    }finally{await this.request('POST',`${this.groupPath(topic,options.group)}/leave`,{member:options.member})}
  }
}
