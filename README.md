<p align="center">
  <img src="https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/7cdf46b9-8871-4c28-90d1-d6e373f0d694" />
</p>

# Project Name: Co-pilot_PNR_OD_Construction



# Team Name: Data Dragons

<img alt="GIF" src="https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/a561c336-142d-415e-94bd-dd034b478c83" style="width:400px;" />
<img align="right" alt="GIF" src="https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/c20ad41c-cc96-46dc-bd9f-b3faa16befc4" style="width:150px;" />


# Introduction
Origin and Destination planning is at the heart of airline operations, influencing route networks, scheduling, and customer service. Airlines strive to optimize O&D dynamics to meet the diverse needs of passengers, enhance connectivity, and ultimately, gain a competitive edge in the global market.

# 1. Input
The solution is using the datasets available at below location-

PNR sample - /mnt/ppeedp/raw/competition/pnr_sample

Distance master - /mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master

Location master - /mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master

Backtrack exception master - /mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster

# 2. Output

![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/fa0c6f61-ad9f-4cf3-adfd-ffeb5deaf425)

## Prompt to CoPilot
# Prompt 1 :-
import all the required libraries for pyspark application and include python math and re library.

# Defining the landing zone file paths to Copilot( input we gave to copilot)

PNR sample - /mnt/ppeedp/raw/competition/pnr_sample

Distance master - /mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master

Location master - /mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master

Backtrack exception master - /mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster

# Prompt 2 :-
Read the Delta files from given path to a pyspark dataFrames

# Prompt 3  :-

## Structure of pnr_sample_df ( Prompt to Copilot)
![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/d6381a04-85bf-4b8b-8661-40d49b57846a)

 ## Structure of distance_master_df ( Prompt to Copilot)

![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/461feccb-ba0e-469f-afa8-ddbeba8b5b76)

## Structure of location_master_df ( Prompt to Copilot)

![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/57f4f934-6d7e-40fc-988c-d046f0a568ca)

## Structure of backtrack_exception_master_df ( Prompt to Copilot)

![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/a36c208f-2f96-44ef-aceb-a6eed506eab9)

# Prompt 4 :-
# Join pnr_sample_df with distance_master on (Prompt to Copilot)
 (ORIG_AP_CD = airport_origin_code and DESTN_AP_CD = airport_destination_code) or  (ORIG_AP_CD = airport_destination_code  and DESTN_AP_CD = airport_origin_code)

# Prompt 5 :- 
# Join the resultant dataframe with location_master on ORIG_AP_CD = ap_cd_val (Prompt to Copilot) 

# Prompt 6 :- 
# Rename column ap_cd_val with ap_cd_val_origin, latitude with latitude_origin and longitude with longitude_origin (Prompt to Copilot)

# Prompt 7 :-
# Join the new_Joined_df with location_master on DESTN_AP_CD = ap_cd_val (Prompt to Copilot) 

# Prompt 8 :-
# Rename the below column (Prompt to Copilot)
ap_cd_val with ap_cd_val_desti, latitude with latitude_desti and longitude with longitude_desti, distance_in_km with distance_in_km_OD and column id with id_OD

# Prompt 9 :-
# Create a Pyspark UDF to convert DMS to Decimal degress
This udf should take one parameter DMS, it should calculates the decimal degrees based on degrees, minutes, seconds, direction values and returns the result. If the direction is either 'S' or 'W', it negates the result because these directions represent negative values in the coordinate system.

# Prompt 10 :-
# Apply the UDF to all the latitude and longitude columns (Prompt to Copilot)

# Prompt 11 :- 
# Write a Python udf to calculate the distance between two points using their latitude and longitude, and this should take lat and lon for both points. This udf should return the distance in kilometers using the Haversine formula.

Haversine formula:
dlon = lon2 - lon1
dlat = lat2 - lat1
a = sin^2(dlat/2) + cos(lat1) * cos(lat2) * sin^2(dlon/2)
c = 2 * atan2(sqrt(a), sqrt(1-a))
distance = R * c
where:
- lon1, lon2, lat1, and lat2 are in radians
- R is the radius of the Earth

# Prompt 12 :- 
Write a pyspark to create a new column which return the sum of value in distance_in_km_OD col over a window of "PNRHash".

# Prompt 13 :- 
Apply the haversine_udf to calculate distance_in_km and OD_total_distance.

# Note :- Please refer the code file for all the remaining prompts in detail.

# Efforts Estimation Metrics :- 

![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/0a6a0aee-c825-4629-a99b-f20a09320035)

# Execution Metrics :-
![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/68c91fed-0d32-4054-957b-dc18f0e71c80)

# Non functional requirements achieved: 
- ⚡ **Reliability** - The job is restartable in case of any failures or reprocessing.

- ⚡ **Performance** - Processing of the job is completed with in given time limit of 30 mins.

- ⚡ **Data engineering standards**  - All the Code blocks are annotated with proper comments,optimization and parameterization of the code is also inplaced with all data engineering standards.












