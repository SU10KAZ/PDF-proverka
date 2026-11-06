"""Reproducible arithmetic receipts; never infer engineering correctness from arithmetic."""
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def number(raw):
    s=str(raw).strip().replace(',','.')
    if not re.fullmatch(r'[+-]?\d+(?:\.\d+)?',s):raise ValueError('Explicit finite decimal required')
    v=Decimal(s)
    if not v.is_finite() or abs(v)>Decimal('1e30'):raise ValueError('Operand outside supported bounds')
    return v


def calculate(operator,values):
    if not 1<=len(values)<=200:raise ValueError('Invalid operand count')
    values=[number(v) for v in values]
    with localcontext() as ctx:
        ctx.prec=40
        if operator=='sum':return str(sum(values))
        if operator=='product':
            value=Decimal(1)
            for v in values:value*=v
            return str(value)
        if operator=='divide' and len(values)==2 and values[1]!=0:return str(values[0]/values[1])
    raise ValueError('Unsupported operation or zero denominator')


def execute(corpus,operation):
    if not operation.get('id') or not operation.get('meaning') or not operation.get('result_unit'):
        raise ValueError('Operation identity, purpose and result unit required')
    operands=operation.get('operands',[])
    for operand in operands:
        bid=operand['block_id'];block=corpus.blocks[bid]
        if block['kind']!='TEXT':raise ValueError('Graphic operand requires separate verified transcription; not auto-grounded')
        quote=operand['quote'];start=operand['offset'];end=start+len(quote)
        if not quote or not isinstance(start,int) or start<0 or block['text'][start:end]!=quote:
            raise ValueError('Operand quote does not match exact authoritative source span')
        # A quoted number is not proof that the model selected the right object,
        # regime or unit. Record that limitation rather than silently promoting it.
        tokens=re.findall(r'(?<![\w.,])[+-]?\d+(?:[.,]\d+)?(?![\w.,])',quote)
        if number(operand['value']) not in [number(t) for t in tokens]:raise ValueError('Operand value not in quoted span')
        if not operand.get('unit'):raise ValueError('Explicit operand unit required')
    value=calculate(operation['operator'],[o['value'] for o in operands])
    body={'schema':1,'operation':operation,'source_blocks_sha256':{
        o['block_id']:hashlib.sha256(corpus.blocks[o['block_id']]['text'].encode()).hexdigest() for o in operands},
        'result':value,'executor':'decimal_host_v1','precision':40,
        'claim_limit':'Arithmetic execution and exact source spans only; object identity, operating regime, unit interpretation and formula applicability unverified.'}
    return {**body,'receipt_sha256':digest(body)}


def verify(corpus,operation,receipt):
    try:return receipt==execute(corpus,operation)
    except (ValueError,KeyError,TypeError,InvalidOperation):return False
