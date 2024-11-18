from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count,countDistinct,desc, sum, row_number
from src.config import ANALYSIS_CONFIG


class Analysis:
    """
    class with methods for respective analysis task
    """

    @staticmethod
    def analysis_1(df: DataFrame) -> int:
        """
        Task: Number of crashes where males killed > 2. 
        Assumption: Death_cnt is a flag, ALIVE - 0, KILLED - 1

        param: 
            df: input dataFrame
        return:
            Count of such crashes
        """
        try:
            #filter male deaths
            male_death_records = df.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") == 1))
            
            # groupby crash_id to total deaths > 2
            male_death_crashes = (male_death_records.groupBy("CRASH_ID")
                                  .agg(sum("DEATH_CNT").alias("TOTAL_DEATHS"))
                                  .filter(col("TOTAL_DEATHS") > 2)).count()
            
            return male_death_crashes
        
        except Exception as e:
            raise RuntimeError(f"Error in analysis_1: {e}")

    @staticmethod
    def analysis_2(df: DataFrame) -> int:
        """
        Task: Count of two-wheelers booked for crashes.
        Assumption: Where vehicle body style contains 'MOTORCYCLE'

        param df: 
            df: input dataFrame
        return: 
            Count of two-wheelers
        """
        try:
            two_wheeler_keywords = ANALYSIS_CONFIG['two_wheeler_keywords']
            
            # filter two wheelers records
            two_wheeler_records = df.filter(col("VEH_BODY_STYL_ID").isin(two_wheeler_keywords)).count()

            return two_wheeler_records
        
        except Exception as e:
            raise RuntimeError(f"Error in analysis_2: {e}")

    @staticmethod
    def analysis_3(primary_person_df: DataFrame,units_df: DataFrame) -> DataFrame:
        """
        Task: Top 5 vehicle makes involved in crashes where drivers died, and airbags didn't deploy
        Assumption: PRSN_TYPE_ID must contain 'DRIVER'

        param df: 
            primary_person_df: primary_person dataframe
            units_df: input units_use dataframe        
        return: 
            dataFrame of top 5 vehicle makes and their counts
        """
        try:
            driver_keywords = ANALYSIS_CONFIG['driver_keywords']
            
            # filter driver with fatalities
            drivers_with_fatalities = primary_person_df.filter(
                 (col("PRSN_TYPE_ID").isin(driver_keywords)) &
                 (col("DEATH_CNT") == 1) &
                 (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED"))
            
            # join with units dataframe to get VEH_MAKE_ID
            filtered_df = drivers_with_fatalities.join(units_df, on=["CRASH_ID", "UNIT_NBR"], how="inner")

            # group by VEH_MAKE_ID and count 
            result = (filtered_df.groupBy("VEH_MAKE_ID")
                      .agg(countDistinct("CRASH_ID", "UNIT_NBR").alias("COUNT"))
                      .orderBy(col("COUNT").desc()).limit(5))

            return result.select("VEH_MAKE_ID", "COUNT")

        except Exception as e:
            raise RuntimeError(f"Error in analysis_3: {e}")

    @staticmethod
    def analysis_4(primary_person_df: DataFrame,units_df: DataFrame) -> DataFrame:
        """
        Task: Vehicles with valid licenses involved in hit-and-run cases.

        param :
            primary_person_df: primary_person dataframe
            units_df: input units_use dataframe
        return: 
            dataFrame of vehicles
        """
        try:
            valid_license_categories = ANALYSIS_CONFIG["valid_license_categories"]
            driver_keywords = ANALYSIS_CONFIG['driver_keywords']

            # filter hit and run cases 
            hit_and_run_df = units_df.filter(col("VEH_HNR_FL") == "Y")

            # join with primary person to get license details
            joined_df = hit_and_run_df.join(primary_person_df, on=["CRASH_ID", "UNIT_NBR"], how="inner")

            # filter for drivers with valid licenses
            valid_license_df = joined_df.filter(
                        (col("PRSN_TYPE_ID").isin(driver_keywords)) & 
                        (col("DRVR_LIC_TYPE_ID").isin(valid_license_categories))).count()
            
            return valid_license_df
        
        except Exception as e:
            raise RuntimeError(f"Error in analysis_4: {e}")

    @staticmethod
    def analysis_5(primary_person_df: DataFrame) -> str:
        """
        Task: State with the highest number of crashes where females were not involved.

        param primary_person_df: 
            input dataFrame
        return: 
            state with the highest number of crashes
        """
        try:

            no_female_involved_crashes = primary_person_df.filter(col("PRSN_GNDR_ID") != "FEMALE")

            # Group by state and count distinct crashes
            top_ranked_state = ( no_female_involved_crashes.groupBy("DRVR_LIC_STATE_ID")
                                    .agg(countDistinct("CRASH_ID").alias("COUNT_CRASH"))
                                    .orderBy(desc("COUNT_CRASH"))).first()
            
            return top_ranked_state["DRVR_LIC_STATE_ID"]

            
        except Exception as e:
            raise RuntimeError(f"Error in analysis_5: {e}")
    
    @staticmethod
    def analysis_6(units_df: DataFrame) -> DataFrame:
        """
        Task: Top 3rd to 5th VEH_MAKE_IDs that contrubute to largest injuries including death. 

        param units_df: 
            input dataFrame
        return:
            dataframe of VEH_MAKE_IDs with counts
        """
        try:
            injuries_df = units_df.filter((col("TOT_INJRY_CNT").isNotNull()) & (col("DEATH_CNT").isNotNull())).withColumn("TOTAL_INJRY_DEATH_CNT", col("TOT_INJRY_CNT") + col("DEATH_CNT"))

            # group by VEH_MAKE_ID
            harm_by_veh_make_id = (
                injuries_df.groupBy("VEH_MAKE_ID")
                .agg(sum("TOTAL_INJRY_DEATH_CNT").alias("TOTAL_HARM")))
            
            # rank records over TOTAL_HARM 
            window_spec = Window.orderBy(desc("TOTAL_HARM"))
            ranked_df = harm_by_veh_make_id.withColumn("RANK", row_number().over(window_spec))

            harm_3_to_5_df = ranked_df.filter((col("RANK") >= 3) & (col("RANK") <= 5)).select("VEH_MAKE_ID", "TOTAL_HARM")
        
            return harm_3_to_5_df
        
        except Exception as e:
            raise RuntimeError(f"Error in analysis_6: {e}")
        
    @staticmethod
    def analysis_7(primary_person_df: DataFrame,units_df: DataFrame) -> DataFrame:
        """
        Task: For body styles involved in crashes, mention the top ethnic group of each unique body style.
        Assumption: removed invalid body types and ethnicity 
        param df:
            primary_person_df: primary_person dataframe
            units_df: input units_use dataframe
        return: 
            dataframe with top ethnic group of each body uniqe style
        """
        try:
            # filter out invalid body styles
            invalid_body_styles = ANALYSIS_CONFIG['invalid_body_style']            
            valid_body_style_df = units_df.filter(~col("VEH_BODY_STYL_ID").isin(invalid_body_styles))

            # filter out invalid ethnicity
            invalid_ethnicity_id = ANALYSIS_CONFIG['invalid_ethnicity_id']
            valid_ethnicity_df = primary_person_df.filter(~col("PRSN_ETHNICITY_ID").isin(invalid_ethnicity_id))

            # join filtered ethnicity and body styles
            joined_df = valid_body_style_df.join(valid_ethnicity_df, on=["CRASH_ID", "UNIT_NBR"], how="inner")

            # group by body and ethnicity and then count crashes
            grouped_df = (joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
                        .agg(count("CRASH_ID").alias("CRASH_COUNTS")))
            
            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("CRASH_COUNTS"))

            # add rank column to identify
            ranked_df = grouped_df.withColumn("RANK", row_number().over(window_spec))

            # select first ethnic group for each body style
            top_ethnic_group_df = ranked_df.filter(col("RANK") == 1).select(
                col("VEH_BODY_STYL_ID").alias("Body_Style"),
                col("PRSN_ETHNICITY_ID").alias("Top_Ethnic_Group"))
            
            return top_ethnic_group_df
            
        except Exception as e:
            raise RuntimeError(f"Error in analysis_7: {e}")
        
    @staticmethod
    def analysis_8(primary_person_df: DataFrame,units_df: DataFrame) -> DataFrame:
        """
        Task:  Top 5 Zip Codes with highest number of car crashes with alcohol as factor.
        Assumption: Crashed car signifies passenger car only , excluding truck , buses, ambulance and etc.
        param df:
            primary_person_df: primary_person dataframe
            units_df: input units_use dataframe
        return: 
            state with the highest number of crashes
        """
        try:
            # filter positive alcohol test
            positive_alcohol_results = primary_person_df.filter((col("PRSN_ALC_RSLT_ID") == "Positive") & (col("DRVR_ZIP").isNotNull()))

            # filter cars only 
            crashed_cars_df = units_df.filter(col("VEH_BODY_STYL_ID").isin("PASSENGER CAR, 2-DOOR","PASSENGER CAR,4-DOOR") & (col("UNIT_DESC_ID") == "MOTOR VEHICLE"))

            car_crashes_with_alcohol = positive_alcohol_results.join(crashed_cars_df, on=["CRASH_ID", "UNIT_NBR"], how="inner")

            crash_counts_zip = (car_crashes_with_alcohol.groupBy("DRVR_ZIP").
                                  agg(count("CRASH_ID").alias("CRASH_COUNT")).orderBy(desc("CRASH_COUNT")))

            # get top 5 zip codes
            top_5_zip_codes_df = crash_counts_zip.limit(5)

            return top_5_zip_codes_df

        except Exception as e:
            raise RuntimeError(f"Error in analysis_8: {e}")
        
    @staticmethod
    def analysis_9(damages_df: DataFrame,units_df: DataFrame) -> str:
        """
        Task:  
            Count distinct Crash IDs where:
            1. No damaged property was observed.
            2. Damage level (VEH_DMAG_SCL~) is above 4 > DAMAGED 4
            3. Vehicle avails valid insurance.

        Assumption: Crashed car signifies passenger car only , excluding truck , buses, ambulance and etc.
        param df:
            damages_df: damages_use dataframe
            units_df: units_use dataframe
        return: 
            state with the highest number of crashes
        """
        try:
            valid_insurance = ANALYSIS_CONFIG['valid_insurance']
            
            # filter for crashes with no damaged property
            undamaged_property = damages_df.filter(
                (col("DAMAGED_PROPERTY").isNull()) | (col("DAMAGED_PROPERTY") == "")
            )

            # filter for level 5 and valid insurance in Units table
            damaged_valid_insurance = units_df.filter(
                ((col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4") | (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")) &  
                (col("FIN_RESP_TYPE_ID").isin(valid_insurance)))

            # join damage and units tables on CRASH_ID
            joined_df = undamaged_property.join(
                damaged_valid_insurance, on="CRASH_ID", how="inner"
            )

            #  distinct crashids
            distinct_crash_count = joined_df.select("CRASH_ID").distinct().count()

            return distinct_crash_count

        except Exception as e:
            raise RuntimeError(f"Error in analysis_9: {e}")
        
    @staticmethod
    def analysis_10(charges_df: DataFrame,primary_person_df: DataFrame,units_df: DataFrame) -> DataFrame:
        """
        Task: 
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding-related offenses, have licensed drivers, used top 10 vehicle colors, and are licensed in the top 25 states withthe highest number of offenses.

        param df:
            damages_df: damages_use dataframe
            units_df: units_use dataframe
        return: 
            state with the highest number of crashes
        """
        try:
            
            # filter for charges with speeding 
            speeding_offenses = charges_df.filter(col("CHARGE").contains("SPEEDING"))

            # filter for licensed drivers
            valid_licenses = ANALYSIS_CONFIG['valid_license_categories']
            driver_keywords = ANALYSIS_CONFIG['driver_keywords']
            licensed_drivers = primary_person_df.filter((col("PRSN_TYPE_ID").isin(driver_keywords)) 
                                                        & col("DRVR_LIC_TYPE_ID").isin(valid_licenses))

            # filter top 10 colors
            top_ten_vehicle_colors = units_df.groupBy("VEH_COLOR_ID").agg(count("*").alias("CLR_COUNT")).orderBy(desc("CLR_COUNT")).limit(10)

            top_ten_colors = [row["VEH_COLOR_ID"] for row in top_ten_vehicle_colors.collect()]

            vehicles_with_top_colors = units_df.filter(col("VEH_COLOR_ID").isin(top_ten_colors))

            # filter top 25 states with high offences
            top_25_states = (primary_person_df.groupBy("DRVR_LIC_STATE_ID")
                .agg(count("*").alias("STATE_COUNT"))
                .orderBy(desc("STATE_COUNT"))
                .limit(25))
            top_states_list = [row["DRVR_LIC_STATE_ID"] for row in top_25_states.collect()]

            vehicles_in_top_states = primary_person_df.filter(col("DRVR_LIC_STATE_ID").isin(top_states_list))

            filtered_df = vehicles_with_top_colors.join(speeding_offenses, on=["CRASH_ID", "UNIT_NBR"]).join(vehicles_in_top_states, on=["CRASH_ID", "UNIT_NBR"]).join(licensed_drivers, on=["CRASH_ID", "UNIT_NBR"])

            top_5_vehicle_makes = (filtered_df.groupBy("VEH_MAKE_ID")
                             .agg(count("*").alias("OFFENSE_COUNT"))
                             .orderBy(desc("OFFENSE_COUNT"))
                             .limit(5))
            
            return top_5_vehicle_makes

        except Exception as e:
            raise RuntimeError(f"Error in analysis_10: {e}")
            
