import logging

from sarus_data_spec.constants import (
    BIG_DATA_TASK,
    IS_BIG_DATA,
    TO_SQL_CACHING_TASK,
    SQL_CACHING_URI,
    PROTECTION_TASK,
    CACHE_PATH,
)
from sarus_data_spec.context.worker import WorkerContext
from sarus_data_spec.dataset import sql

from sarus_data_spec.transform import (
    assign_budget,
    attributes_budget,
    automatic_budget,
    automatic_protected_paths,
    automatic_public_paths,
    automatic_user_settings,
    protect,
    user_settings,
)
import sarus_data_spec.status as stt
import sarus_data_spec.protobuf as sp
from sarus_data_spec.attribute import attach_properties
from sarus_data_spec.path import path, paths

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

with WorkerContext() as context:
    URI = "postgresql+psycopg2://postgres:1234@localhost:5432/retail"
    tables = [('', 'features'), ('', 'sales'), ('', 'stores')]
    pths = paths([['features', 'id'], ['sales', 'id'], ['stores', 'store']])

    ds = sql(uri=URI, tables=tables)

    # all external statuses have to be set for all managers sharing
    # the storage
    stt.ready(ds, task=BIG_DATA_TASK, properties={IS_BIG_DATA: str(False)})
    stt.ready(ds, task=TO_SQL_CACHING_TASK, properties={SQL_CACHING_URI: URI})

    protected_paths = automatic_protected_paths()(ds)
    attach_properties(
        dataspec=protected_paths,
        name=PROTECTION_TASK,
        properties={
            CACHE_PATH: sp.utilities.to_base64(
                path(
                    label='data',
                    paths=pths,
                ).protobuf()
            )
        },
    )
    public_entities = automatic_public_paths()(ds)
    protected_ds = protect()(
        ds, protected_paths=protected_paths, public_paths=public_entities
    )
    print(protected_ds)
    protected_ds.to_pandas()
    user_type = automatic_user_settings()(protected_ds)
    final_ds = user_settings()(protected_ds, user_type=user_type)
    final_ds.to_pandas()

    # BUDGETS Definitions
    total_budget = automatic_budget()(final_ds)
    attr_budget = attributes_budget()(total_budget)
    ds_with_budget = assign_budget()(final_ds, attributes_budget=attr_budget)

    # compute bounds
    schema = ds_with_budget.bounds().statistics().protobuf()

    dataset_ = str(ds_with_budget)
    with open("demo/retail_data/dataset.json", "w") as f:
        f.write(dataset_)

    size_ = str(ds_with_budget.size())
    with open("demo/retail_data/size.json", "w") as f:
        f.write(size_)

    stats = str(ds_with_budget.schema())
    with open("demo/retail_data/schema.json", "w") as f:
        f.write(stats)
    print(stats)

