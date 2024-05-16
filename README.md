![image](https://github.com/ayushdixit487/Co-pilot_PNR_OD_Construction/assets/25612446/7cdf46b9-8871-4c28-90d1-d6e373f0d694) 
# Co-pilot_PNR_OD_Construction

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









