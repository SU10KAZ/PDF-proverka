"""Receipted bounded calls for ownership reasoning over admitted local states."""
import json
import time

from experiments.project_change_272.inventory import immutable
from .packets import digest
from .run import MODEL,ESTIMATED_CALL_CEILING_USD


class LocalCalls:
    def __init__(self,out):
        from openai import AsyncOpenAI
        from backend.app.core.config import OPENROUTER_API_KEY,OPENROUTER_BASE_URL
        self.client=AsyncOpenAI(api_key=OPENROUTER_API_KEY,base_url=OPENROUTER_BASE_URL,max_retries=0,timeout=120)
        self.out=out;self.receipts=[]

    async def call(self,key,system,data):
        from backend.app.services.llm.paid_api_guard import PaidApiContext,reserve_paid_api,release_reservation
        from backend.app.services.common.usage_service import paid_cost_tracker
        body=json.dumps(data,ensure_ascii=False)
        if len(system)+len(body)>60000:raise ValueError('Local state request exceeds bound')
        messages=[dict(role='system',content=system),dict(role='user',content=body)]
        request=dict(model=MODEL,messages=messages,max_tokens=6500,temperature=0)
        immutable(self.out/'requests'/(key+'.json'),request)
        reservation=reserve_paid_api(PaidApiContext(source='offline_research.project_change_272',model=MODEL,
            project_id='272_Sadovnicheskaya_76_Balchug_Esteyt',stage='ownership',job_id=key,
            estimated_cost_usd=ESTIMATED_CALL_CEILING_USD))
        start=time.monotonic()
        try:
            response=await self.client.chat.completions.create(**request,response_format={'type':'json_object'},
                extra_body={'reasoning':{'effort':'low'},'provider':{'data_collection':'deny'}})
            usage=response.usage.model_dump() if response.usage else {}
            receipt=dict(request_hash=digest(request),response_id=response.id,usage=usage,
                response=response.choices[0].message.content or '',finish_reason=response.choices[0].finish_reason,
                seconds=time.monotonic()-start,provider_cost_usd=usage.get('cost'),input_characters=len(system)+len(body))
            immutable(self.out/'calls'/(key+'.json'),receipt);self.receipts.append(receipt)
            cost=usage.get('cost')
            if isinstance(cost,(float,int)) and cost>0:
                paid_cost_tracker.record_paid(cost,model=MODEL,project_id='272_Sadovnicheskaya_76_Balchug_Esteyt',
                    stage='ownership',source='offline_research.project_change_272',job_id=key,
                    input_tokens=usage.get('prompt_tokens',0),output_tokens=usage.get('completion_tokens',0),
                    response_id=response.id,extra={'research_receipt':str(self.out/'calls'/(key+'.json'))})
            if receipt['finish_reason']!='stop':return {}
            try:return json.loads(receipt['response'])
            except (ValueError,TypeError):return {}
        finally:release_reservation(reservation)

    async def close(self):
        await self.client.close()
        immutable(self.out/'COST.json',dict(calls=len(self.receipts),
            input_tokens=sum(r['usage'].get('prompt_tokens',0) for r in self.receipts),
            output_tokens=sum(r['usage'].get('completion_tokens',0) for r in self.receipts),
            known_provider_cost_usd=sum(r['provider_cost_usd'] for r in self.receipts if isinstance(r['provider_cost_usd'],(float,int))),
            calls_with_unknown_cost=sum(not isinstance(r['provider_cost_usd'],(float,int)) for r in self.receipts)))
