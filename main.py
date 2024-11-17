from pyspark.sql import SparkSession
from src.data_ingestion import DataIngestion
from src.analysis import Analysis
from src.config import FILES
from src.utils import save_result


def main():
    """
    driver function for running car_crash analysis.
    """
    # spark session initialization
    spark = SparkSession.builder.appName("Crash_Analysis").getOrCreate()

    # data ingestion initialization
    ingestion = DataIngestion(spark)

    # load datasets
    primary_person_df = ingestion.load_csv(FILES["Primary_Person_use"])
    units_df = ingestion.load_csv(FILES["Units_use"])
    damages_df = ingestion.load_csv(FILES["Damages_use"])
    charges_df = ingestion.load_csv(FILES["Charges_use"])

    try:
        # Analysis 1: Number of crashes where males killed > 2
        analysis_1_result = Analysis.analysis_1(primary_person_df)
        save_result(analysis_1_result,"Analysis_1")
        print(f"Analysis 1: Completed --- ")
        
        
        # Analysis 2: Count of two-wheelers booked for crashes
        analysis_2_result = Analysis.analysis_2(units_df)
        save_result(analysis_2_result,"Analysis_2")
        print(f"Analysis 2: Completed --- ")
        
        # Analysis 3: Top 5 vehicle makes where drivers died, airbags didn't deploy
        analysis_3_result = Analysis.analysis_3(primary_person_df, units_df)
        save_result(analysis_3_result,"Analysis_3")
        print(f"Analysis 3: Completed --- ")
        
        # Analysis 4: Vehicles with valid licenses involved in hit-and-run cases
        analysis_4_result = Analysis.analysis_4(primary_person_df, units_df)
        save_result(analysis_4_result,"Analysis_4")
        print(f"Analysis 4: Completed --- ")
    
        # Analysis 5: State with the highest number of accidents excluding females
        analysis_5_result = Analysis.analysis_5(primary_person_df)
        save_result(analysis_5_result, "Analysis_5")
        print(f"Analysis 5: Completed --- ")
        
        # Analysis 6: 3rd to 5th VEH_MAKE_IDs contributing to the largest number of injuries (including deaths)
        analysis_6_result = Analysis.analysis_6(units_df)
        save_result(analysis_6_result, "Analysis_6")
        print(f"Analysis 6: Completed --- ")

        # Analysis 7: Top ethnic user group for each body style involved in crashes
        analysis_7_result = Analysis.analysis_7(primary_person_df, units_df)
        save_result(analysis_7_result, "Analysis_7")
        print(f"Analysis 7: Completed --- ")
        
        # Analysis 8: Top 5 ZIP codes with the highest number of alcohol-related crashes
        analysis_8_result = Analysis.analysis_8(primary_person_df,units_df)
        save_result(analysis_8_result, "Analysis_8")
        print(f"Analysis 8: Completed --- ")
        
        # Analysis 9: Count of distinct crash IDs where no damaged property was observed,
        # and damage level is above 4, with car insurance
        analysis_9_result = Analysis.analysis_9(damages_df, units_df)
        save_result(analysis_9_result, "Analysis_9")
        print(f"Analysis 9: Completed --- ")
    
        # Analysis 10: Top 5 vehicle makes where drivers are charged with speeding offenses,
        # have valid licenses, use top 10 vehicle colors, and are licensed in top 25 states
        analysis_10_result = Analysis.analysis_10(charges_df,primary_person_df, units_df)
        save_result(analysis_10_result, "Analysis_10")
        print(f"Analysis 10: Completed --- ")

        print("Crash_Analysis Completed.")
 
    except Exception as e:
        print(f"Error during analysis execution: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()