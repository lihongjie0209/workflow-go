import os, sys

basedir = "D:/code/workflow-go/storage"

def patch_sql_store(filepath, ph):
    with open(filepath, 'r', encoding='utf-8') as f:
        c = f.read()

    # Schema: add business_key
    old_schema = "def_id TEXT NOT NULL,\n\t\tstate TEXT NOT NULL DEFAULT 'running'"
    new_schema = "def_id TEXT NOT NULL,\n\t\tbusiness_key TEXT NOT NULL DEFAULT '',\n\t\tstate TEXT NOT NULL DEFAULT 'running'"
    if old_schema in c:
        c = c.replace(old_schema, new_schema)

    # INSERT
    if ph == '?':
        c = c.replace(
            "INSERT INTO process_instances (id, def_id, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            "INSERT INTO process_instances (id, def_id, business_key, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
    else:
        c = c.replace(
            "INSERT INTO process_instances (id, def_id, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            "INSERT INTO process_instances (id, def_id, business_key, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        )
    c = c.replace(
        "pi.ID, pi.ProcessDefinitionID, string(pi.State)",
        "pi.ID, pi.ProcessDefinitionID, pi.BusinessKey, string(pi.State)"
    )

    # GetProcessInstance SELECT
    c = c.replace(
        "SELECT def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE id = ",
        "SELECT business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE id = "
    )
    # ListProcessInstances SELECT
    c = c.replace(
        "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE def_id = ",
        "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE def_id = "
    )
    # ListCompleted SELECT
    c = c.replace(
        "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE state = 'completed'",
        "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE state = 'completed'"
    )
    # QueryProcessInstances SELECT
    c = c.replace(
        "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances ",
        "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances "
    )

    # scanProcessInstances var block
    c = c.replace(
        "var (\n\t\t\tid, defID, stateStr, varsJSON, parentPIID, parentActID string\n\t\t\tstartedAt                     time.Time\n\t\t\tendedAt                       *time.Time\n\t\t)",
        "var (\n\t\t\tbusinessKey                   string\n\t\t\tid, defID, stateStr, varsJSON, parentPIID, parentActID string\n\t\t\tstartedAt                     time.Time\n\t\t\tendedAt                       *time.Time\n\t\t)"
    )
    # scan args
    c = c.replace(
        "if err := rows.Scan(&id, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID); err != nil {",
        "if err := rows.Scan(&id, &businessKey, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID); err != nil {"
    )
    # GetProcessInstance scan
    c = c.replace(
        "Scan(&defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID)",
        "Scan(&businessKey, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID)"
    )

    # Add BusinessKey to result structs
    if "BusinessKey: businessKey," not in c:
        c = c.replace(
            "ParentProcessInstanceID: parentPIID,",
            "BusinessKey: businessKey,\n\t\t\tParentProcessInstanceID: parentPIID,"
        )
        c = c.replace(
            "BusinessKey: businessKey,\n\t\t\tParentProcessInstanceID: parentPIID,",
            "BusinessKey: businessKey,\n\t\t\tParentProcessInstanceID: parentPIID,"
        )

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(c)
    print("OK: " + filepath)


# mysqlstore schema is different - separate stmts with VARCHAR
mysql_path = basedir + "/mysqlstore/mysql_store.go"
with open(mysql_path, 'r', encoding='utf-8') as f:
    mc = f.read()

old_mysql_schema = "def_id VARCHAR(255) NOT NULL,\n\t\t\tstate VARCHAR(50) NOT NULL DEFAULT 'running'"
new_mysql_schema = "def_id VARCHAR(255) NOT NULL,\n\t\t\tbusiness_key VARCHAR(255) NOT NULL DEFAULT '',\n\t\t\tstate VARCHAR(50) NOT NULL DEFAULT 'running'"
mc = mc.replace(old_mysql_schema, new_mysql_schema)

# INSERT
mc = mc.replace(
    "INSERT INTO process_instances (id, def_id, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
    "INSERT INTO process_instances (id, def_id, business_key, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)
mc = mc.replace(
    "pi.ID, pi.ProcessDefinitionID, string(pi.State)",
    "pi.ID, pi.ProcessDefinitionID, pi.BusinessKey, string(pi.State)"
)

# SELECT columns: same patterns as sqlstore
mc = mc.replace(
    "SELECT def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE id = ",
    "SELECT business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE id = "
)
mc = mc.replace(
    "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE def_id = ",
    "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE def_id = "
)
mc = mc.replace(
    "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE state = 'completed'",
    "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE state = 'completed'"
)
mc = mc.replace(
    "SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances ",
    "SELECT id, business_key, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances "
)

# scanProcessInstances
mc = mc.replace(
    "var (\n\t\t\tid, defID, stateStr, varsJSON, parentPIID, parentActID string\n\t\t\tstartedAt                     time.Time\n\t\t\tendedAt                       *time.Time\n\t\t)",
    "var (\n\t\t\tbusinessKey                   string\n\t\t\tid, defID, stateStr, varsJSON, parentPIID, parentActID string\n\t\t\tstartedAt                     time.Time\n\t\t\tendedAt                       *time.Time\n\t\t)"
)
mc = mc.replace(
    "if err := rows.Scan(&id, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID); err != nil {",
    "if err := rows.Scan(&id, &businessKey, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID); err != nil {"
)
mc = mc.replace(
    "Scan(&defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID)",
    "Scan(&businessKey, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID)"
)
if "BusinessKey: businessKey," not in mc:
    mc = mc.replace(
        "ParentProcessInstanceID: parentPIID,",
        "BusinessKey: businessKey,\n\t\t\tParentProcessInstanceID: parentPIID,"
    )

with open(mysql_path, 'w', encoding='utf-8') as f:
    f.write(mc)
print("OK: " + mysql_path)

# Patch sqlstore and pgstore
patch_sql_store(basedir + "/sqlstore/sql_store.go", "?")
patch_sql_store(basedir + "/pgstore/pg_store.go", "$")
