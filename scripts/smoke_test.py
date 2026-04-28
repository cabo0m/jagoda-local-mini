from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import local_core


def main() -> None:
    print('init_db:', local_core.init_db())
    created = local_core.create_private_memory(
        content='Local-mini smoke memory: Jagoda pamięta lokalnie.',
        memory_type='smoke_test',
        owner_user_key='michal',
        summary_short='Local-mini smoke memory',
        tags='smoke,local-mini',
        project_key='local-mini',
    )
    memory_id = int(created['memory']['id'])
    print('created:', memory_id)
    found = local_core.find_memories('smoke', user_key='michal', workspace_key='default', limit=5)
    print('found_count:', found['count'])
    got = local_core.get_visible_memory(memory_id, user_key='michal', workspace_key='default')
    print('got_summary:', got['memory']['summary_short'])
    recalled = local_core.recall_memory(memory_id, user_key='michal', workspace_key='default')
    print('recall_count:', recalled['memory']['recall_count'])
    restored = local_core.restore_jagoda_core(project_key='local-mini')
    print('restore:', restored['status'], restored['name'])
    print('OK')


if __name__ == '__main__':
    main()
