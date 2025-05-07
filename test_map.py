# prompt: I want to visualize and map the results of the following query: {query}.
# Please help me with the code to display how the value changes over time based on the timestamp

import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.animation import FuncAnimation
from db_manager import get_db_connection

# Query the data from DuckDB
con = get_db_connection()
query = """
SELECT
    latitude,
    longitude,
    value,
    valid_time_utc
FROM data.main.hrrr_forecasts
WHERE variable = 'visible_beam_downward_solar_flux'
"""
df = con.execute(query).fetch_df()

# Convert valid_time_utc to datetime for sorting
df["valid_time_utc"] = pd.to_datetime(df["valid_time_utc"])

# Sort the dataframe by valid_time_utc
df = df.sort_values("valid_time_utc")

# Create a figure and axis for plotting
fig = plt.figure(figsize=(12, 6))
ax = plt.axes(projection=ccrs.PlateCarree())

# Add features like coastline, borders, and states
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS)
ax.add_feature(cfeature.STATES, edgecolor="gray")

# Create a scatter plot that will be updated during the animation
sc = ax.scatter([], [], c=[], cmap="viridis", s=80, edgecolor="k", transform=ccrs.PlateCarree())


# Function to update the plot
def update(frame):
    # Filter the data for the current time step
    current_time = df["valid_time_utc"].unique()[frame]
    time_data = df[df["valid_time_utc"] == current_time]

    # Update the scatter plot with new data
    sc.set_offsets(list(zip(time_data["longitude"], time_data["latitude"])))
    sc.set_array(time_data["value"].values)

    # Update the title with the current time
    ax.set_title(f"Visible Solar Flux at Forecast Points - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    return (sc,)


# Create the animation
ani = FuncAnimation(fig, update, frames=len(df["valid_time_utc"].unique()), interval=500, repeat=False)

# Show the animation
plt.colorbar(sc, label="Visible Beam Downward Solar Flux")
plt.show()
