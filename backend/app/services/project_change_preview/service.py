"""Read-only presentation of release-bundled ProjectChange data. No research imports."""
from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path
import threading
from functools import lru_cache

OBJECT = '4f3e5916'  # Canonical production /api/objects registry ID; the only API gate.
SNAPSHOT_OBJECT = '272_Sadovnicheskaya_76_Balchug_Esteyt'  # Frozen provenance, never a request alias.
SNAPSHOT = Path(__file__).resolve().parents[2] / 'data/project_change_preview_snapshot_v3'

class SourceUnavailable(ValueError):
    pass

class PreviewService:
    def __init__(self, root=SNAPSHOT):
        self.root=Path(root).resolve()
        self._lock=threading.RLock()
        self._stats={}
        self.manifest=json.loads((self.root/'MANIFEST.json').read_text())
        m=self.manifest
        if (m.get('schema')!='project-change-production-snapshot/1' or m.get('object_id')!=SNAPSHOT_OBJECT
                or m.get('decision_mode')!='READ_ONLY' or m.get('auto_refresh') is not False):
            raise SourceUnavailable('Invalid immutable preview manifest')
        self.receipts=dict(m['files'])
        self.receipts['MANIFEST.json']=self.sha(self.root/'MANIFEST.json')
        self.assert_current()
        self.data=json.loads(self.path('presentation.json').read_text())
        env=self.data['envelope']
        if env['object_id']!=SNAPSHOT_OBJECT or env['decision_revision']!=0 or any(i['effective_decision'] for i in env['items']):
            raise SourceUnavailable('Preview must contain no engineer decisions')
        self.sources=self

    @staticmethod
    def sha(path):
        with path.open('rb') as f: return hashlib.file_digest(f,'sha256').hexdigest()

    def path(self,relative):
        p=self.root/relative
        if p.is_symlink() or not p.resolve().is_relative_to(self.root) or relative not in self.receipts:
            raise SourceUnavailable('File outside snapshot')
        return p

    def assert_current(self):
        with self._lock:
            for name,digest in self.receipts.items():
                p=self.path(name);s=p.stat();stamp=(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
                if self._stats.get(name)!=stamp:
                    if self.sha(p)!=digest: raise SourceUnavailable('Immutable preview changed')
                    self._stats[name]=stamp

    def envelope(self,report_only=False):
        self.assert_current()
        out=copy.deepcopy(self.data['envelope'])
        # Adapt only transport identity; keep the sealed snapshot and evidence intact.
        out['object_id']=OBJECT
        old_prefix=f'/api/project-change-preview/objects/{SNAPSHOT_OBJECT}/'
        new_prefix=f'/api/project-change-preview/objects/{OBJECT}/'
        for item in out['items']:
            for evidence in item['evidence']:
                url=evidence['image_url']
                if url.startswith(old_prefix):
                    evidence['image_url']=new_prefix+url[len(old_prefix):]
        out['capabilities'].update(decisions=False,history=False)
        if report_only:
            out['items']=[i for i in out['items'] if i['status']=='CONFIRMED' and i['effective_decision']]
        return out

    def pair(self,pair_id):
        self.assert_current()
        return copy.deepcopy(self.data['pairs'][pair_id])

    def _document(self,pair_id,side,page):
        if side not in {'old','new'} or type(page) is not int or page<1: raise KeyError('Page')
        doc=self.data['documents'][pair_id+':'+side]
        if page in doc['embargo_pages']: raise KeyError('Page outside preview scope')
        return doc,self.path(doc['file'])

    def page_info(self,pair_id,side,page):
        import fitz
        self.assert_current();meta,path=self._document(pair_id,side,page)
        with fitz.open(path) as doc:
            if page>len(doc): raise KeyError('Page')
            p=doc[page-1]
            return {'width':p.rect.width,'height':p.rect.height,'rotation':p.rotation,'page_count':len(doc),
                    'signature':meta['source_sha256']}

    def crop(self,evidence_id):
        self.assert_current();e=self.data['evidence'][evidence_id]
        crop_file=e.get('crop_file')
        if crop_file:
            p=self.root/crop_file
            if p.is_file() and p.resolve().is_relative_to(self.root):
                return p.read_bytes()
        _,path=self._document(e['pair_id'],e['side'],e['page'])
        return self._raster(str(path),e['page'],tuple(e['box']) if e['box'] else None,1200,None)

    def page_raster(self,pair_id,side,page,width=1400,tile=None):
        self.page_info(pair_id,side,page);_,path=self._document(pair_id,side,page)
        return self._raster(str(path),page,None,width,tuple(tile) if tile else None)

    @lru_cache(maxsize=96)
    def _raster(self,path,page,box,width,tile):
        import fitz
        with self._lock,fitz.open(path) as doc:
            p=doc[page-1];clip=fitz.Rect(box) if box else p.rect
            scale=min(3,width/clip.width)
            if tile is not None:
                level,x,y=tile;scale=2**level
                clip=fitz.Rect(x*512/scale,y*512/scale,(x+1)*512/scale,(y+1)*512/scale)&p.rect
                if clip.is_empty: raise KeyError('Tile outside page')
            return p.get_pixmap(matrix=fitz.Matrix(scale,scale),clip=clip,alpha=False).tobytes('png')
