import requests
from utils import get_s3_path

run_date_dt = "2025-01-01"
forecast_hour = 48
s3_url = get_s3_path(run_date_dt, forecast_hour)



idx_url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20250101/conus/hrrr.t06z.wrfsfcf00.grib2.idx"
# For your actual code, ensure your S3_BUCKET_URL and path construction are correct.
# This example uses the direct HTTPS path for 'requests'.

try:
    response = requests.get(idx_url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    idx_content = response.text.splitlines()

    print(f"Fetched {len(idx_content)} lines from {idx_url}\nFirst 20 lines:")
    for line in idx_content[:20]:
        print(line)

    print("\nSearching for potential matches for your variables:")
    # Keywords to search for in the .idx descriptions
    # Format of .idx line: line_num:byte_offset:date_info:VAR_ABBREV:LEVEL_INFO:forecast_info
    search_terms = {
        "surface_pressure": ["PRES:surface"],
        "surface_roughness": ["SFCR:surface"],
        # "visible_beam_downward_solar_flux": ["VBDSF:surface"],
        # "visible_diffuse_downward_solar_flux": ["VDDSF:surface"],
        # "temperature_2m": ["TMP:2 m above ground"],
        # "dewpoint_2m": ["DPT:2 m above ground", "DEWPT:2 m above ground"],
        # "relative_humidity_2m": ["RH:2 m above ground"],
        # "u_component_wind_10m": ["UGRD:10 m above ground"],
        # "v_component_wind_10m": ["VGRD:10 m above ground"],
        # "u_component_wind_80m": ["UGRD:80 m above ground"],
        # "v_component_wind_80m": ["VGRD:80 m above ground"],
    }

    found_vars_info = {}

    for line_idx, line_content in enumerate(idx_content):
        parts = line_content.split(":")
        if len(parts) > 4:
            var_abbrev = parts[3]
            level_info = parts[4]
            description_key = f"{var_abbrev}:{level_info}"

            for human_name, terms_to_match in search_terms.items():
                if description_key in terms_to_match:
                    if human_name not in found_vars_info:  # Take the first match
                        found_vars_info[human_name] = {
                            "idx_line_num": parts[0],
                            "var_abbrev_idx": var_abbrev,
                            "level_info_idx": level_info,
                            "full_idx_line": line_content,
                        }

    print("\n--- Found Info from .idx ---")
    for name, info in found_vars_info.items():
        print(
            f"For '{name}': Found in .idx as '{info['var_abbrev_idx']}:{info['level_info_idx']}' (Line {info['idx_line_num']})"
        )
        print(f"  Full line: {info['full_idx_line']}")
        # Now you can use info['var_abbrev_idx'] and info['level_info_idx']
        # to guide your VARIABLE_MAP_GUIDANCE values for `shortName` and `typeOfLevel`/`level`.

except requests.exceptions.RequestException as e:
    print(f"Error fetching .idx file: {e}")
