#!/usr/bin/env python
# coding: utf-8

import os
import pygsheets
from google.oauth2 import service_account
import json
import requests
import pandas as pd
import numpy as np
from geopy.geocoders import ArcGIS


def restaurant_data():
    def nyc_restaurants():
        url = "https://data.cityofnewyork.us/api/odata/v4/43nn-pn8j"
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                json_data = response.json()
                if '@odata.nextLink' in json_data:
                    url = json_data["@odata.nextLink"]
                    yield pd.DataFrame(json_data['value'])
                else:
                    yield pd.DataFrame(json_data['value'])
                    break
                    
    restaurant_inspection = pd.concat(list(nyc_restaurants()), axis = 0).reset_index(drop = True).drop(columns = ['__id','record_date','location_point1'])
    restaurant_inspection['inspection_date'] = pd.to_datetime(restaurant_inspection['inspection_date'])
    restaurant_inspection = restaurant_inspection.rename(columns = {"camis": "permit_number", "dba": "restaurant_name", "boro": "borough"})
    restaurant_inspection = restaurant_inspection[restaurant_inspection["inspection_date"]!='1900-01-01T00:00:00.000']
    restaurant_inspection.replace('None', np.nan, inplace=True)
    return restaurant_inspection
                   
def replace_lat_lon(row):
    def lat_lon_geocode(address):
        geolocator_arcgis = ArcGIS()
        location = geolocator_arcgis.geocode(address)
        if location!=None:
            return location.latitude, location.longitude
        else:
            return np.nan, np.nan
        
    if pd.isna(row["latitude"]) and pd.isna(row["longitude"]):
        if pd.isna(row["building"]):
            address = row["street"] + ", " + row["borough"] + ", NY"
        else:
            address = row["building"] + row["street"] + ", " + row["borough"] + ", NY" 
        geoloc = lat_lon_geocode(address)
        row["latitude"] = geoloc[0]
        row["longitude"] = geoloc[1]
    return row

def reop_inconsistent_count(row):
    count = 0
    if pd.notna(row["grade"]):
        if row["action"]=="Establishment re-opened by DOHMH.":
            filtered = restaurant_inspection[(restaurant_inspection["permit_number"]==row["permit_number"])&(restaurant_inspection["action"].isin(["Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.","Establishment re-closed by DOHMH."]))].sort_values(by="inspection_date", ascending=False)
            inspections_when_closed = filtered.groupby(["permit_number","inspection_date"])["inspection_type"].agg(list).reset_index().sort_values(by=["permit_number","inspection_date"], ascending=[True, False])
            for index in range(len(inspections_when_closed)):
                inspection_list = inspections_when_closed.iloc[index,:]["inspection_type"]
                if 'Cycle Inspection / Initial Inspection' in inspection_list or 'Pre-permit (Operational) / Initial Inspection' in inspection_list:
                    if row["grade"] not in ["P","Z"]:
                        count += 1
                    break
                elif 'Cycle Inspection / Re-inspection' in inspection_list or 'Pre-permit (Operational) / Re-inspection' in inspection_list:
                        re_inspection_filtered = filtered[filtered["inspection_type"].isin(re_inspection)].iloc[0,:]
                        if pd.isna(re_inspection_filtered["grade"]):
                            if re_inspection_filtered["score"] >= 14 and re_inspection_filtered["score"] <=28 and row["grade"] not in ["B","Z"]:
                                count += 1
                            elif re_inspection_filtered["score"] > 28 and row["grade"] not in ["C","Z"]:
                                count += 1
                        break
                elif any("Compliance Inspection" in inspection for inspection in inspection_list):
                    if row["grade"] != "C":
                        count += 1
                    break
                elif any("Reopening Inspection" in inspection for inspection in inspection_list):
                    continue
        elif row["action"] in ["Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.","Establishment re-closed by DOHMH."] and row["grade"] != "N":
            count += 1
    return count 

def gradable_inspection(df):
    def determine_grade(row):
        initial_inspection = ['Cycle Inspection / Initial Inspection','Pre-permit (Operational) / Initial Inspection']
        re_inspection = ['Cycle Inspection / Re-inspection','Pre-permit (Operational) / Re-inspection']
        if pd.isna(row['grade']):
            if pd.notna(row['score']):
                if row['score'] <= 13 and row["inspection_type"] in initial_inspection+re_inspection:
                    return 'A'
                elif row['score'] > 13 and row["inspection_type"] in initial_inspection:
                    return 'N'
                elif row["score"] >= 14 and row["score"] <= 28 and row["inspection_type"] in re_inspection:
                    return 'B'
                elif row["score"] > 28 and row["inspection_type"] in re_inspection:
                    return 'C'
                elif row["inspection_type"] in ['Pre-permit (Operational) / Reopening Inspection','Cycle Inspection / Reopening Inspection']:
                    if row["action"] == 'Establishment re-opened by DOHMH.':
                        filtered = restaurant_inspection[(restaurant_inspection["permit_number"]==row["permit_number"])&(restaurant_inspection["action"].isin(["Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.","Establishment re-closed by DOHMH."]))].sort_values(by="inspection_date", ascending=False)
                        inspections_when_closed = filtered.groupby(["permit_number","inspection_date"])["inspection_type"].agg(list).reset_index().sort_values(by=["permit_number","inspection_date"], ascending=[True, False])
                        for index in range(len(inspections_when_closed)):
                            inspection_list = inspections_when_closed.iloc[index,:]["inspection_type"]
                            if 'Cycle Inspection / Initial Inspection' in inspection_list or 'Pre-permit (Operational) / Initial Inspection' in inspection_list:
                                return 'P'
                                break
                            elif 'Cycle Inspection / Re-inspection' in inspection_list or 'Pre-permit (Operational) / Re-inspection' in inspection_list:
                                re_inspection_filtered = filtered[filtered["inspection_type"].isin(re_inspection)].iloc[0,:]
                                if pd.isna(re_inspection_filtered["grade"]):
                                    if re_inspection_filtered["score"] >= 14 and re_inspection_filtered["score"] <28:
                                        return 'B'
                                    elif re_inspection_filtered["score"] >= 28:
                                        return 'C'
                                else:
                                    return re_inspection_filtered["grade"]
                                break
                            elif any("Compliance Inspection" in inspection for inspection in inspection_list):
                                return 'C'
                                break
                            elif any("Reopening Inspection" in inspection for inspection in inspection_list):
                                continue
                    else:
                        return "N"
        elif pd.notna(row["grade"]):
            closed_action = ['Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.','Establishment re-closed by DOHMH.']
            if row["inspection_type"] in re_inspection and row["action"] not in closed_action and row["grade"]=="N":
                if row["score"] <= 13:
                    return 'A'
                elif row["score"] >= 14 and row["score"] <= 28:
                    return 'B'
                elif row["score"] > 28:
                    return 'C'

            elif row["inspection_type"] in ['Pre-permit (Operational) / Reopening Inspection','Cycle Inspection / Reopening Inspection']:
                if row["action"] == 'Establishment re-opened by DOHMH.':
                    filtered = restaurant_inspection[(restaurant_inspection["permit_number"]==row["permit_number"])&(restaurant_inspection["action"].isin(["Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.","Establishment re-closed by DOHMH."]))].sort_values(by="inspection_date", ascending=False)
                    inspections_when_closed = filtered.groupby(["permit_number","inspection_date"])["inspection_type"].agg(list).reset_index().sort_values(by=["permit_number","inspection_date"], ascending=[True, False])
                    for index in range(len(inspections_when_closed)):
                        inspection_list = inspections_when_closed.iloc[index,:]["inspection_type"]
                        if 'Cycle Inspection / Initial Inspection' in inspection_list or 'Pre-permit (Operational) / Initial Inspection' in inspection_list:
                            return 'P'
                            break
                        elif any("Compliance Inspection" in inspection for inspection in inspection_list):
                            return 'C'
                            break
                        elif any("Reopening Inspection" in inspection for inspection in inspection_list):
                            continue
                else:
                    return "N"

        return row['grade']
    
    condition1 = df["inspection_type"].isin(['Cycle Inspection / Initial Inspection','Pre-permit (Operational) / Initial Inspection'])
    condition2 = df["inspection_type"].isin(['Cycle Inspection / Re-inspection','Pre-permit (Operational) / Re-inspection'])
    condition3 = df["inspection_type"].isin(['Pre-permit (Operational) / Reopening Inspection','Cycle Inspection / Reopening Inspection'])
    gradable_inspections = df[condition1|condition2|condition3]
    gradable_inspections = gradable_inspections.sort_values(by=["permit_number","inspection_date"], ascending = [True, False]).drop_duplicates(subset="permit_number")
    gradable_inspections["grade"] = gradable_inspections.apply(determine_grade, axis=1)
    gradable_inspections.loc[gradable_inspections["action"]=="No violations were recorded at the time of this inspection.","grade"] = "A"
    gradable_inspections.loc[gradable_inspections["grade"]=="N","grade"]="Not Yet Graded"
    gradable_inspections.loc[gradable_inspections["grade"].isin(["Z","P"]),"grade"] = "Grade Pending"
    return gradable_inspections
                
def data_preprocessing(df, gc):
    def custom_title(input_str, preserve_chars):
        words = input_str.split()
        result = []
        for word in words:
            if any(char.isalpha() for char in word):
                preserved_word = ''.join(char if char in preserve_chars else char.lower() for char in word)
                result.append(preserved_word.capitalize())
            else:
                result.append(word)
        return ' '.join(result)

    def img_link(grade):
        if grade == "A":
            img_src = "https://a816-health.nyc.gov/ABCEatsRestaurants/Content/images/NYCRestaurant_A.svg"
        elif grade == "B":
            img_src = "https://a816-health.nyc.gov/ABCEatsRestaurants/Content/images/NYCRestaurant_B.svg"
        elif grade == "C":
            img_src = "https://a816-health.nyc.gov/ABCEatsRestaurants/Content/images/NYCRestaurant_C.svg"
        elif grade == "Grade Pending":
            img_src = "https://a816-health.nyc.gov/ABCEatsRestaurants/Content/images/NYCRestaurant_GP.svg"
        else:
            img_src = "https://a816-health.nyc.gov/ABCEatsRestaurants/Content/images/NYCRestaurant_NG.svg"
        return img_src
    
    def street_name_converter(street):
        words = street.split()
        corrected_words = [word.capitalize() if word.isalpha() else word.replace("ST","st").replace("ND","nd").replace("RD","rd").replace("TH","th") for word in words]
        street = ' '.join(corrected_words)
        address_split = street.split()
        for word in address_split:
            if word == "West":
                street = street.replace("West","W")
            elif word == "East":
                street = street.replace("East","E")
            elif word == "Street":
                street = street.replace("Street","St")
            elif word == "Avenue":
                street = street.replace("Avenue","Ave")
            elif word == "Boulevard":
                street = street.replace("Boulevard","Blvd")
            elif word == "Road":
                street = street.replace("Road", "Rd")
            elif word == "Parkway":
                street = street.replace("Parkway", "Pkwy")
            elif word == "Turnpike":
                street = street.replace("Turnpike","Tpke")
            elif word == "Saint":
                street = street.replace("Saint","St")
            elif word == "Wyckoff":
                street = street.replace("Wyckoff","Wyck")
            elif word == "Expressway":
                street = street.replace("Expressway", "Expy")
            elif word == "Place":
                street = street.replace("Place","Pl")
            elif word ==  "Lane":
                street = street.replace("Lane","Ln")
            elif word == "B'WAY":
                street = street.replace("B'WAY","Broadway")
            elif word == "Bldg":
                street = street.replace("Bldg","Building")
            elif word == "intairp":
                street = street.replace("intairp","International Airport")
            elif word == "Arvl":
                street = street.replace("Arvl", "Arrival")
            elif word == "Highway":
                street = street.replace("Highway","Hwy")
            elif word == "Ctr":
                street = street.replace("Ctr","Center")
            elif word == "Drive":
                street = street.replace("Drive","Dr")
            elif word == "Plz":
                street = street.replace("Plz","Plaza")
            elif word == "Terrace":
                street = street.replace("Terrace","Ter")
            elif word == "Square":
                street = street.replace("Square","Sq")
            elif word == "Traverse":
                street = street.replace("Traverse","Transverse")
            elif word == "Jfk":
                street = street.replace("Jfk","John F. Kennedy")
            else:
                try:
                    number = int(word)
                    if number % 10 == 1:
                        street = street.replace(word, str(number)+"st")
                    elif number % 10 == 2:
                        street = street.replace(word, str(number)+"nd")
                    elif number % 10 == 3:
                        street = street.replace(word, str(number)+"rd")
                    else:
                        street = street.replace(word, str(number)+"th")
                except ValueError:
                    continue
        return street
        
    workbook = gc.open('nyc_restaurant_inspections') 
    prev_restaurant = workbook[0].get_as_df(numerize=False)
    prev_restaurant = prev_restaurant.replace("", np.nan)
    prev_restaurant[["latitude","longitude"]] = prev_restaurant[["latitude","longitude"]].astype(float)
    restaurant = df[["permit_number","restaurant_name","borough",
                                        "building","street","zipcode","latitude",
                                        "longitude","phone","cuisine_description"]]
    restaurant = restaurant.drop_duplicates(ignore_index = True)
    restaurant[["latitude","longitude"]] = restaurant[["latitude","longitude"]].replace(0, np.nan)
    restaurant = restaurant.set_index(["permit_number"]).fillna(prev_restaurant.set_index(["permit_number"])).reset_index()
    restaurant.loc[restaurant['latitude'].isnull() | restaurant['longitude'].isnull()] = restaurant.loc[restaurant['latitude'].isnull() | restaurant['longitude'].isnull()].apply(replace_lat_lon, axis=1)
    gradable_inspections = gradable_inspection(df)
    restaurant = pd.merge(restaurant, gradable_inspections[["permit_number","grade"]],on="permit_number",how="left")
    restaurant["grade"] = restaurant["grade"].fillna("Not Yet Graded")
    restaurant["restaurant_name"] = restaurant["restaurant_name"].apply(lambda x: custom_title(x, preserve_chars="'"))
    restaurant["street"] = restaurant["street"].apply(lambda x: street_name_converter(x))
    restaurant['address'] = restaurant.apply(lambda row: row['street'] if pd.isna(row['building']) else f"{row['building']} {row['street']}", axis=1)
    restaurant["img_src"] = restaurant["grade"].apply(lambda x: img_link(x))
    restaurant = restaurant.drop(columns=["building"])
    violation = df[["permit_number","inspection_date",
                                        "inspection_type","action", "violation_code",
                                        "violation_description","critical_flag","score","grade","grade_date"]]
    return restaurant, violation



restaurant_inspection = restaurant_data()
try:
    json_file = os.environ["JSON_SECRET"]
except KeyError:
    json_file = "Json File Not Available"
SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
credentials = service_account.Credentials.from_service_account_info(json.loads(json_file, strict=False), scopes=SCOPES)
gc = pygsheets.authorize(custom_credentials=credentials)
restaurant, violation = data_preprocessing(restaurant_inspection, gc)

workbook = gc.open('nyc_restaurant_inspections')
for worksheet in workbook:
    worksheet.clear()
workbook[0].set_dataframe(restaurant, start = 'A1', nan = "")
workbook[1].set_dataframe(violation, start = 'A1', nan = "") #Add rows up to dataframe length in google sheets first before applying this code


#DISCREPANCIES BETWEEN SCORES AND GRADES 
#condition1 = restaurant_inspection["inspection_type"].isin(['Cycle Inspection / Initial Inspection','Pre-permit (Operational) / Initial Inspection'])
#condition2 = restaurant_inspection["inspection_type"].isin(['Cycle Inspection / Re-inspection','Pre-permit (Operational) / Re-inspection'])
#condition3 = restaurant_inspection["inspection_type"].isin(['Pre-permit (Operational) / Reopening Inspection','Cycle Inspection / Reopening Inspection'])
#gradable_inspections = restaurant_inspection[condition1|condition2|condition3]
#gradable_inspections = gradable_inspections.sort_values(by=["permit_number","inspection_date"], ascending = [True, False]).drop_duplicates(subset="permit_number")
#initial_inspection = ['Cycle Inspection / Initial Inspection','Pre-permit (Operational) / Initial Inspection']
#re_inspection = ['Cycle Inspection / Re-inspection','Pre-permit (Operational) / Re-inspection']
#reop_inspection = ['Pre-permit (Operational) / Reopening Inspection','Cycle Inspection / Reopening Inspection']
#initial_filtered = gradable_inspections[gradable_inspections["inspection_type"].isin(initial_inspection)]
#re_filtered = gradable_inspections[gradable_inspections["inspection_type"].isin(re_inspection)]
#reop_filtered = gradable_inspections[gradable_inspections["inspection_type"].isin(reop_inspection)]
#inconsistent_initinspect = len(initial_filtered[(initial_filtered["grade"].isin(["B","C","N"]))&(initial_filtered["score"]<=13)].drop_duplicates(subset=["permit_number","inspection_date"]))+len(initial_filtered[(initial_filtered["grade"].isin(["A","B","C"]))&(initial_filtered["score"]>=14)&(initial_filtered["score"]<=27)].drop_duplicates(subset=["permit_number","inspection_date"])) + len(initial_filtered[(initial_filtered["grade"].isin(["A","B","C"]))&(initial_filtered["score"]>=28)].drop_duplicates(subset=["permit_number","inspection_date"]))
#inconsistent_reinspect = len(re_filtered[(re_filtered["grade"].isin(["B","C","N","Z","P"]))&(re_filtered["score"]<=13)].drop_duplicates(subset=["permit_number","inspection_date"]))+len(re_filtered[(re_filtered["grade"].isin(["A","C","N"]))&(re_filtered["score"]>=14)&(re_filtered["score"]<=27)].drop_duplicates(subset=["permit_number","inspection_date"])) + len(re_filtered[(re_filtered["grade"].isin(["A","B","N"]))&(initial_filtered["score"]>=28)].drop_duplicates(subset=["permit_number","inspection_date"]))
#reop_filtered["count"] = reop_filtered.apply(reop_inconsistent_count, axis=1)
#inconsistent_reop = reop_filtered["count"].sum()
#inconsistent_matches = inconsistent_initinspect + inconsistent_reinspect + inconsistent_reop + len(gradable_inspections[(gradable_inspections["grade"]=="A")&(gradable_inspections["score"]==0)&(gradable_inspections["action"]=='Violations were cited in the following area(s).')])
#total_records = len(gradable_inspections[(~gradable_inspections["grade"].isna())&(~gradable_inspections["score"].isna())])
#print(f"Percentage of unmatched score and grade {inconsistent_matches/total_records}")



