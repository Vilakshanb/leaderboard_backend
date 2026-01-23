
import logging
import os
import pymongo
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime

# Raw data from user (Tab-separated)
RAW_DATA = """
MPF11640	Dwarka express way	Public Transport	Shivangi Jindal	Sagar Maini	2025-04-07
MPR0726	B 301 Satisar Apartment Plot no 6 Sector- 7 Dwarka 110075	Personal Transport	VINOD KUMAR BAZAZ	Yatin Munjal	2025-04-12
MPF11664	Clients office, B-102, Pushpanjali Enclave, Pitampura, New Delhi-110034	Personal Transport	Surender Mohan Hora	Ishu Mavar	2025-04-15
MPF11665	Pushpanjali	Public Transport	Anil kumar Saxena	Sagar Maini	2025-04-15
MPR0021	Hardik Kathpalia’s office	Personal Transport	HARDIK KATHPALIA	Pramod Bhutani	2025-04-17
MPR14917	Office of Mr. Baljeet Singh at Noorveer Creation	Personal Transport	BALJEET SINGH NOORVEER	Pramod Bhutani	2025-04-17
MPF11672	Rohini sector 8	Public Transport	Dr Saurabh Manish	Sagar Maini	2025-04-21
MPR8609	Rohini sector -8	Metro	AMARESHWAR NARAYAN	Sagar Maini	2025-04-21
MPF11674	South delhi	Personal Transport	Ashish Sethi	Sagar Maini	2025-04-23
MPR0060	Saraswati vihar	Metro	SANJAY KUMAR GOEL	Sagar Maini	2025-04-23
MPF11677	F-345, Bank steet, Chatri Vala Kuan, Lado Sarai, New Delhi-110030	Personal Transport	Ashish Sethi	Ishu Mavar	2025-04-23
MPF11676	Client’s residence, C565, Saraswati Vihar, Delhi-110034	Personal Transport	Pranika Goel	Ishu Mavar	2025-04-23
MPF11683	Rohini sector-8	Public Transport	Ankur Khurana	Sagar Maini	2025-04-26
MPF11685	Karol bagh	Public Transport	Vikas Bhalla	Sagar Maini	2025-04-26
MPF11636	Karol Bagh	Public Transport	Ajay bhatia	Sagar Maini	2025-04-29
MPF11693	Connaught place	Public Transport	NItee Sharma	Sagar Maini	2025-04-30
MPF11707	Noida sec-45	Public Transport	RAJENDRA TRIPATHI	kawal Singh Jailwal	2025-05-09
MPF11708	HARYANA BAHADURGARH	Public Transport	raju batra raghav batra mother	kawal Singh Jailwal	2025-05-11
MPF11711	Chanakya puri	Public Transport	Vikram singh Rana	Sagar Maini	2025-05-12
MPF11712	Noida	Public Transport	Dilip Associate	Sagar Maini	2025-05-12
MPF11717	Chandni chowk	Public Transport	Sanjay kumar jain	Sagar Maini	2025-05-14
MPF11720	Paschim vihar	Public Transport	Rajeev Tuteja	Sagar Maini	2025-05-15
MPF11721	Shalimar Bagh	Public Transport	Gauri Ref Harsh	Sagar Maini	2025-05-15
MPF11719	Client’s residence, D5/145, Sector 7, Rohini, Delhi-110085	Personal Transport	Anju Sethia	Ishu Mavar	2025-05-15
MPF11346	Dadri	Public Transport	Priyesh Bhardwaj	Sagar Maini	2025-05-17
MPF11723	Shiv Nadar Institution of Deemed Excellence, NH 91, Tehsil Dadri, Greater Noida, Uttar Pradesh- 201314	Personal Transport	Piyush Bhardwaj	Ishu Mavar	2025-05-17
MPF11707	Noida sec-45	Personal Transport	RAJENDRA TRIPATHI	kawal Singh Jailwal	2025-05-19
MPF11728	Ajay Unisex Saloon, 1st floor, C10, Amar Colony Market, Lajpat Nagar, Delhi- 110048	Personal Transport	Ajay Saloon	Ishu Mavar	2025-05-21
MPF11730	Noida sec-45	Public Transport	Mr.Mohit chauhan ref Sir	kawal Singh Jailwal	2025-05-23
MPF11732	Gurgaon	Public Transport	Nishant Mathur	Sagar Maini	2025-05-26
MPF11735	Client’s factory, Naresh Engg Works, E-70, Sector 2, Bawana, Delhi-110039	Personal Transport	Pawan Jangir	Ishu Mavar	2025-05-27
MPF11739	Client’s residence, 638/21, Sector 23, Gurugram, Haryana-122022	Personal Transport	Shivam Kaushik	Ishu Mavar	2025-05-30
MPF11760	Dwarka	Public Transport	Abhishek Ref Ankit Tandon	Sagar Maini	2025-06-05
MPF11724	Sector 8 rohiini	Public Transport	Dr. SAURABH MANISH	Sagar Maini	2025-06-05
MPF11762	Client’s residence, 6025/3, Pocket 6, Sector D, Vasant Kunj, Delhi-110070	Personal Transport	Ravinder Kaur Chawla	Ishu Mavar	2025-06-07
MPF11772	Mukherjee Nagar	Public Transport	Sahaj Preet Ref Bawa	Sagar Maini	2025-06-14
MPF11681	Safdarjung Enclave	Public Transport	Ramakant sharma	kawal Singh Jailwal	2025-06-18
MPF11636	KAROL BAGH	Public Transport	Ajay bhatia	Sagar Maini	2025-06-18
MPF11786	Dwarka sector 11	Ola/Uber/Meru	Pabitra Nazir	Sagar Maini	2025-06-25
MPF11792	Noida	Public Transport	Vikas. Chandra	Sagar Maini	2025-06-28
MPF11807	Shahdara	Public Transport	Gaurav Bhatnagar Ref Ankit Tandon	Sagar Maini	2025-07-02
MPF11849	McDonald’s, Naraina, New Delhi	Personal Transport	Rabinarayan Mohanty	Ishu Mavar	2025-07-05
MPF11856	Rajouri Garden	Ola/Uber/Meru	Hitesh Madan	Sagar Maini	2025-07-08
MPF11809	Clinic Eximus, 88, Jagriti Enclave Rd, near Jain Heart Hospital, Karkardooma, Anand Vihar, Delhi, 110092	Personal Transport	Neha Sharma	Ishu Mavar	2025-07-08
MPF11860	Shiv Shakti Fibre Udyog, A1/23, 1st floor, Near Punjab Sweets, Sector 11, Rohini, Delhi-110085	Personal Transport	Sonam Gupta	Ishu Mavar	2025-07-10
MPF12902	Mcdonalds, 47 Priya Market, Vasant Vihar, Delhi-110057	Personal Transport	Ayush Chakravarti	Ishu Mavar	2025-07-19
MPF12907	Shankar Vihar	Public Transport	Himani Palande	Sagar Maini	2025-07-22
MPF12918	Flat no 255, Astha Kunj, Sector 18, Rohini, Delhi-110085	Personal Transport	Anurag	Ishu Mavar	2025-07-24
MPF12983	Ghaziabad	Public Transport	Sumeet Arora	Sagar Maini	2025-07-31
MPF12996	Uttam Nagar	Ola/Uber/Meru	Rakesh Ref Himanshu Vasudev	Sagar Maini	2025-08-05
MPF13040	Rohini	Public Transport	Subhash Chander Grover	Sagar Maini	2025-08-07
MPF13034	West Block 4, R. K. Puram	Public Transport	Aashu sir-	kawal Singh Jailwal	2025-08-07
MPF13066	Kashmiri gate	Ola/Uber/Meru	Sandeep ref Jatin Dhingra	Sagar Maini	2025-08-12
MPF13068	Shalimar bagh	Public Transport	Om Prakash chhabra	Sagar Maini	2025-08-12
MPF13066	Rohini sector 5	Personal Transport	Sandeep ref Jatin Dhingra	Sagar Maini	2025-08-13
MPF13072	Hyeopseong Engineering Pvt Ltd, 135, Ecotech III, Greater Noida, Habibpur, Uttar Pradesh-201318	Personal Transport	Ashutosh	Ishu Mavar	2025-08-13
MPF12972	Client’s shop, Jay Auto Parts, Shop no 13, Naharpur car market, Sector 7, Rohini, Delhi- 110085	Personal Transport	Virender Goyal	Ishu Mavar	2025-08-20
MPF13057	Client’s residence, B-285, Gali no 11, Adarsh Nagar, Delhi- 110033	Personal Transport	Sarika Malik	Ishu Mavar	2025-08-20
MPF13171	Ramesh nagar	Public Transport	Karan Marwah	Sagar Maini	2025-08-23
MPF11447	Green park	Public Transport	Pankaj Mohanty	Sagar Maini	2025-08-28
MPF13245	Munirka	Public Transport	Rajesh malik ref sir	kawal Singh Jailwal	2025-08-30
MPF13329	Karol bagh	Public Transport	RUCHI BHASIN	kawal Singh Jailwal	2025-09-10
MPF13346	Kavi nagar ghaziabad	Public Transport	Vishal khatri	Sagar Maini	2025-09-11
MPF13303	Sadar bazar	Public Transport	Mukesh Goel Ref Praveen dhingra	Sagar Maini	2025-09-11
MPF13245	Munirka	Public Transport	(Geeta) Rajesh malik ref sir	kawal Singh Jailwal	2025-09-13
MPF13399	shadara	Public Transport	kavita sharama	kawal Singh Jailwal	2025-09-23
MPF13403	Sonipat	Ola/Uber/Meru	Vikas Vashisht Sonipat	Sagar Maini	2025-09-25
MPF13250	Connaught Place	Ola/Uber/Meru	Sunmeet Singh	Sagar Maini	2025-09-26
MPF13416	Client’s residence, 317, Sainik Vihar, Saraswati Vihar, Delhi-110034	Personal Transport	Akshara Aggarwal	Ishu Mavar	2025-10-01
MPF13422	Greater Noida	Public Transport	Dinesh Khare	kawal Singh Jailwal	2025-10-06
MPF13420	Vasant Vihar	Public Transport	Hira Lal Wangnoo	Sagar Maini	2025-10-07
MPF13423	Pitampura	Public Transport	Gulshan Rai	Sagar Maini	2025-10-07
MPF13423	Pitampura	Public Transport	Gulshan Rai	Sagar Maini	2025-10-08
MPF13432	Paschim vihar	Public Transport	Mukul Arora	Sagar Maini	2025-10-14
MPF13421	Vasant vihar	Public Transport	Deepti Gupta	Sagar Maini	2025-10-14
MPF13450	Client’s residence, 1st Floor, House No 322, Sector 40, Gurgaon, Haryana- 122001	Personal Transport	K V Vinodh	Ishu Mavar	2025-11-06
MPF13423	Rohini	Personal Transport	Gulshan Rai	Sagar Maini	2025-11-22
MPF13471	Client’s residence, B-4/171, Yamuna Vihar, Delhi-110053	Personal Transport	Mukesh Kumar Sharma	Ishu Mavar	2025-11-24
MPF13472	Client’s residence, B-4/171, Yamuna Vihar, Delhi-110053	Personal Transport	Mr. Ayush Bhardwaj	Ishu Mavar	2025-11-24
"""

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_secret(name: str):
    if os.getenv(name):
        return os.getenv(name)
    kv_url = "https://milestonetsl1.vault.azure.net/"
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=kv_url, credential=credential)
        return client.get_secret(name).value
    except Exception as e:
        logging.error(f"Failed to fetch secret {name}: {e}")
        return None

def compute_source(mid):
    # Replicate webhook logic
    s = str(mid or "").strip().upper()
    if s.startswith("MPF"):
        return "Investment Lead"
    if s.startswith("MPR"):
        return "Portfolio Review"
    return "Unknown"

def parse_data():
    records = []
    lines = RAW_DATA.strip().split("\n")
    for line in lines:
        parts = line.split("\t")
        if len(parts) >= 6:
            # Columns: ID, Location, Type, Investor, Owner, Date
            # Map columns based on user provided structure
            record = {
                "ID": parts[0].strip(),
                "Location": parts[1].strip(),
                "Type": parts[2].strip(),
                "investor": parts[3].strip(),
                "owner": parts[4].strip(),
                "Date": parts[5].strip()
            }
            # Derive Period (YYYY-MM)
            if len(record["Date"]) >= 7:
                record["Period"] = record["Date"][:7]
            else:
                record["Period"] = ""

            record["source"] = compute_source(record["ID"])
            records.append(record)
    return records

def main():
    conn_str = get_secret("MongoDb-Connection-String")
    if not conn_str:
        logging.error("Could not find MongoDb-Connection-String")
        return

    client = pymongo.MongoClient(conn_str)
    db = client["iwell"]
    col = db["Investor_Meetings_Data"]

    logging.info("Connected to MongoDB.")

    # Clear valid existing data to ensure clean state as requested
    logging.info("Clearing existing records...")
    col.delete_many({})
    logging.info("Collection cleared.")

    logging.info("Starting bulk upload...")

    data = parse_data()
    inserted = 0
    updated = 0

    # We use upsert=True based on ID to avoid dupes but update existing
    for doc in data:
        try:
            res = col.update_one(
                {"ID": doc["ID"]},
                {"$set": doc},
                upsert=True
            )
            if res.upserted_id:
                inserted += 1
            elif res.modified_count > 0:
                updated += 1
        except Exception as e:
            logging.error(f"Error processing {doc['ID']}: {e}")

    logging.info(f"Upload Complete. Inserted: {inserted}, Updated: {updated}, Total in batch: {len(data)}")

if __name__ == "__main__":
    main()
