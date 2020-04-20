#!/usr/bin/env python
""" @name ortime
    @description Import OR times data and output graphs and statistics
    @kind statistics """


from datetime import timedelta
import pandas as pd

data = pd.read_csv(
    "ortime.csv",
    dtype={
        "LOCATIONID": "category",
        "LOCATION": "category",
        "OPERATINGROOMID": "category",
        "OPERATINGROOM": "category",
        "MRNUMBER": "str",
        "PATIENTNAME": "str",
        "BOOKINGTYPEID": "category",
        "DESCRIPTION": "category",
        "PATIENTTYPE": "category",
        "ANESID": "category",
        "PROCEDUREID": "category",
        "PROCEDURE": "category",
        "SURGEONID": "category",
        "NAME": "category",
    },
    parse_dates=[
        "CASEDATE",
        "PATIENTSENTDTTM",
        "PATIENTARRIVEDDTTM",
        "PATIENTINDTTM",
        "PATIENTINDTTM2",
        "PATIENTREADYFORDTTM",
        "PREPSTARTDTTM",
        "INCISIONDTTM",
        "DRESSINGDTTM",
        "TIMEOUTDTTM",
        "RECOVERYINDTTM",
        "RECOVERYOUTDTTM",
    ],
)

data["OPERATINGROOMID"].cat.reorder_categories(
    [
        "IND R",
        "OR1",
        "OR2",
        "OR3",
        "OR4",
        "OR5",
        "OR6",
        "OR7",
        "OR8",
        "OR9",
        "OR10",
        "OR11",
        "OR12",
        "OR13",
        "OR14",
        "OR15",
        "OR16",
        "OR17",
    ],
    inplace=True,
)
data.drop_duplicates(["CASEDATE", "MRNUMBER"], inplace=True)

# drop blank ANESDTTM column
data.drop(columns=['ANESDTTM'], inplace=True)

# calculate day of the week for each CASEDATE
data.insert(
    6,
    "CASEDAY",
    pd.Categorical(data["CASEDATE"].dt.weekday).rename_categories(
        {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }
    ),
)

# correct midnight times incorrectly recorded as 12:XX instead of 00:XX
for i in range(17, 27):
    incorrect_midnights = (data.iloc[:, i] > data.iloc[:, i+1]) & data.iloc[:, i].dt.hour.eq(12)
    data.loc[incorrect_midnights, data.columns[i]] -= timedelta(hours=12)

# correct PM times incorrectly recorded as (for example) 01:XX instead of 13:XX
for i in range(17, 27):
    incorrect_PMs = (data.iloc[:, i] > data.iloc[:, i+1]) & data.iloc[:, i].dt.hour.ne(12)
    data.loc[incorrect_PMs, data.columns[i+1:27]] += timedelta(hours=12)

