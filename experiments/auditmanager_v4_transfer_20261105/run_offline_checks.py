import os,socket,subprocess,sys
os.environ.update({'AUDIT_DISABLE_DOTENV':'1','AUDIT_APP_DATA_DIR':'/tmp/auditmanager_v4_review/test_app_data','AUDIT_OPENROUTER_ENABLED':'false','PAID_API_ENABLED':'false','PYTHONDONTWRITEBYTECODE':'1'})
from pathlib import Path
Path(os.environ['AUDIT_APP_DATA_DIR']).mkdir(exist_ok=True)
def no_network(*a,**k):raise RuntimeError('NETWORK_DISABLED_FOR_OFFLINE_AUDIT')
socket.socket.connect=no_network
socket.socket.connect_ex=no_network
socket.create_connection=no_network
original_popen=subprocess.Popen
def guarded_popen(args,*a,**k):
 text=' '.join(map(str,args)) if isinstance(args,(list,tuple)) else str(args)
 executable = Path(str(args[0])).name if isinstance(args,(list,tuple)) else ''
 if executable not in {'node', 'git'}: raise RuntimeError('MODEL_SUBPROCESS_DISABLED_FOR_OFFLINE_AUDIT: '+executable)
 return original_popen(args,*a,**k)
subprocess.Popen=guarded_popen
import pytest
sys.exit(pytest.main(['-q','tests/test_stage01_evidence_context.py','backend/tests/test_absence_guard.py','tests/test_findings_deterministic_critic.py','tests/test_findings_deterministic_corrector.py','backend/tests/test_findings_verify_stage.py','tests/test_stage02_finding_provenance.py','tests/test_audit_coverage_honesty.py','--junitxml='+str(Path(__file__).with_name('offline_checks.xml'))]))
