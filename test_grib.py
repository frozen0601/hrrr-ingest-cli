import cfgrib
from config import VARIABLE_MAP

grib_file = "hrrr.t06z.wrfsfcf00.grib2"

variables_to_extract = list(VARIABLE_MAP.keys())
index_cache = "cfgrib_index_cache"

selected_vars = {}

for variable in variables_to_extract:
    filter_keys = VARIABLE_MAP[variable]
    try:
        ds = cfgrib.open_dataset(
            grib_file,
            filter_by_keys=filter_keys,
            indexpath=index_cache,  # per-variable index
        )
        # Automatically detect the main data variable
        data_vars = list(ds.data_vars)
        if not data_vars:
            raise ValueError("Dataset has no data variables.")
        var_name = data_vars[0]
        selected_vars[variable] = ds
        print(f"✅ Extracted {variable}: actual var '{var_name}', shape {ds[var_name].shape}")
    except Exception as e:
        print(f"❌ Failed to extract {variable} ({filter_keys.get('shortName')}): {e}")
