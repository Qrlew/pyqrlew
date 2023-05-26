import json
import pandas as pd
import typing as t

from sarus_data_spec.attribute import attach_properties
from sarus_data_spec.constants import (
    BIG_DATA_TASK,
    IS_BIG_DATA,
    TO_SQL_CACHING_TASK,
    SQL_CACHING_URI,
    PROTECTION_TASK,
    CACHE_PATH,
    VALIDATED_TYPE,
)
from sarus_data_spec.dataset import sql
from sarus_data_spec.manager.ops.processor.standard.user_settings.utils import (
    validate_all_types,
)
import sarus_data_spec.typing as st
from sarus_data_spec.transform import (
    assign_budget,
    attributes_budget,
    automatic_budget,
    automatic_protected_paths,
    automatic_public_paths,
    automatic_user_settings,
    protect,
    user_settings,
    validated_user_type,
)
from sarus_data_spec.path import paths, path
import sarus_data_spec.status as stt
import sarus_data_spec.protobuf as sp
from .names import (
    DATASET_FILENAME,
    SCHEMA_FILENAME,
    SIZE_FILENAME,
    BOUNDS_FILENAME,
    URI,
)

def prepare_dataset(uri: str, tables: t.List[t.Tuple[str]], protected_ps) -> st.Dataset:
    ds = sql(uri=uri, tables=tables)
    stt.ready(ds, task=BIG_DATA_TASK, properties={IS_BIG_DATA: str(False)})
    stt.ready(ds, task=TO_SQL_CACHING_TASK, properties={SQL_CACHING_URI: uri})

    user_type = automatic_user_settings()(ds)
    automatic_user_ds = user_settings()(ds, user_type=user_type)
    # Add validation step
    final_user_type = validated_user_type()(automatic_user_ds)
    # create attribute
    validated_types = validate_all_types(user_type.value())
    attach_properties(
        final_user_type,
        name=VALIDATED_TYPE,
        properties={
            VALIDATED_TYPE: json.dumps(validated_types),
        },
    )

    # protected
    user_ds = user_settings()(automatic_user_ds, user_type=final_user_type)
    protected_paths = automatic_protected_paths()(user_ds)
    attach_properties(
        dataspec=protected_paths,
        name=PROTECTION_TASK,
        properties={
            CACHE_PATH: sp.utilities.to_base64(
                path(
                    label='data',
                    paths=protected_ps,
                ).protobuf()
            )
        },
    )
    public_entities = automatic_public_paths()(user_ds)
    protected_ds = protect()(
        user_ds, protected_paths=protected_paths, public_paths=public_entities
    )
    protected_ds.to_pandas()

    # BUDGETS Definitions
    total_budget = automatic_budget()(protected_ds)
    attr_budget = attributes_budget()(total_budget)
    ds_with_budget = assign_budget()(
        protected_ds, attributes_budget=attr_budget
    )
    # Attribute computations
    ds_with_budget.size()
    return ds_with_budget

tables =[('', 'census'), ('', 'beacon')]
protected_paths = paths([['census'], ['beacon', 'UserId']])
ds = prepare_dataset(
    tables=tables,
    uri=URI,
    protected_ps=protected_paths,
)

with open(DATASET_FILENAME, "w") as f:
        f.write(str(ds))
print(f"File {DATASET_FILENAME} written.")

# write schema
with open(SCHEMA_FILENAME, "w") as f:
        f.write(str(ds.schema()))
print(f"File {SCHEMA_FILENAME} written.")

# write bounds
with open(BOUNDS_FILENAME, "w") as f:
    f.write(str(ds.bounds()))
print(f"File {BOUNDS_FILENAME} written.")

# write size
with open(SIZE_FILENAME, "w") as f:
    f.write(str(ds.size()))
print(f"File {SIZE_FILENAME} written.")

res = ds.sql('SELECT COUNT(*) FROM census')
print(pd.DataFrame(res))