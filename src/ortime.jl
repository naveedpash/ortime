module ortime

# Imports
using Revise;

using CSV;
using DataFrames;
using CategoricalArrays;
using Dates;
using StatsKit;
using DataFramesMeta;

import Dates.Time;
import Dates.Minute;
import Dates.value;

# Read and clean the data
data = DataFrame!(CSV.read("data/ortime.csv",
                           dateformat="mm/dd/yy HH:MM"));

# Identify categorical variables...
data = categorical(data, [:LOCATIONID, :LOCATION,
                          :OPERATINGROOMID, :OPERATINGROOM,
                          :BOOKINGTYPEID, :PATIENTTYPE, :ANESID,
                          :PROCEDUREID, :PROCEDURE,
                          :SURGEONID, :NAME]);
# ...and fix levels
levels!(data.OPERATINGROOMID, ["IND R", "OR1", "OR2",
                               "OR3", "OR4", "OR5",
                               "OR6", "OR7", "OR8",
                               "OR9", "OR10", "OR11",
                               "OR12", "OR13", "OR14",
                               "OR15", "OR16", "OR17"]);
# Fix dates
data.CASEDATE .+= Dates.Year(2000);
data.CASEDATE = convert.(Dates.Date, data.CASEDATE);
data.PATIENTSENTDTTM .+= Dates.Year(2000);
data.PATIENTARRIVEDDTTM .+= Dates.Year(2000);
data.PATIENTINDTTM .+= Dates.Year(2000);
data.PATIENTINDTTM2 .+= Dates.Year(2000);
data.PATIENTREADYFORDTTM .+= Dates.Year(2000);
data.PREPSTARTDTTM .+= Dates.Year(2000);
data.INCISIONDTTM .+= Dates.Year(2000);
data.DRESSINGDTTM .+= Dates.Year(2000);
data.TIMEOUTDTTM .+= Dates.Year(2000);
data.RECOVERYINDTTM .+= Dates.Year(2000);
data.RECOVERYOUTDTTM .+= Dates.Year(2000);

# Label Case Days
data.CASEDAY = CategoricalArray(Dates.dayname.(data.CASEDATE));
levels!(data.CASEDAY, ["Monday","Tuesday","Wednesday","Thursday",
                       "Friday","Saturday","Sunday"])

# Clean Up Duplicate MRNUMBERs
# These are the patients with mulitple procedures in the same day
data = unique(data, [:CASEDATE, :MRNUMBER]);

# Calculate times of interest
times = @linq data |>
transform(ORARRIVAL   = convert.(Minute, :PATIENTARRIVEDDTTM - :PATIENTSENTDTTM),
          ORINTAKE    = convert.(Minute, :PATIENTINDTTM - :PATIENTARRIVEDDTTM),
          INDUCTION   = convert.(Minute, :PATIENTREADYFORDTTM - :PATIENTINDTTM),
          PATIENTPREP = convert.(Minute, :PREPSTARTDTTM - :PATIENTINDTTM),
          PREP        = convert.(Minute, :INCISIONDTTM - :PREPSTARTDTTM),
          EXTUBATION  = convert.(Minute, :TIMEOUTDTTM - :DRESSINGDTTM),
          RECOVERYIN  = convert.(Minute, :RECOVERYINDTTM - :TIMEOUTDTTM),
          RECOVERYOUT = convert.(Minute, :RECOVERYOUTDTTM - :RECOVERYINDTTM)) |>
select(:CASEDATE, :CASEDAY, :PATIENTTYPE,
       :OPERATINGROOMID, :PROCEDUREID, :SURGEONID,
       :ORARRIVAL, :ORINTAKE, :INDUCTION, :PATIENTPREP,
       :PREP, :EXTUBATION, :RECOVERYIN, :RECOVERYOUT)

# TODO: Check normality of times of interest and
#       use median/IQR instead of mean/std as appropriate
# TODO: Check for duplicated MRNs and remove as appropriate

# Summarize times of interest by SURGEONID
@linq times |>
by(:SURGEONID,
   MEANARRIVAL     = mean(value.(:ORARRIVAL)),
   SDARRIVAL       =  std(value.(:ORARRIVAL)),
   MEANINTAKE      = mean(value.(:ORINTAKE)),
   SDINTAKE        =  std(value.(:ORINTAKE)),
   MEANINDUCTION   = mean(value.(:INDUCTION)),
   SDINDUCTION     =  std(value.(:INDUCTION)),
   MEANPATPREP     = mean(value.(:PATIENTPREP)),
   SDPATPREP       =  std(value.(:PATIENTPREP)),
   MEANPREP        = mean(value.(:PREP)),
   SDPREP          =  std(value.(:PREP)),
   MEANEXTUBATION  = mean(value.(:EXTUBATION)),
   SDEXTUBATION    =  std(value.(:EXTUBATION)),
   MEANRECOVERYIN  = mean(value.(:RECOVERYIN)),
   SDRECOVERYIN    =  std(value.(:RECOVERYIN)),
   MEANRECOVERYOUT = mean(value.(:RECOVERYOUT)),
   SDRECOVERYOUT   =  std(value.(:RECOVERYOUT)))

# Summarize times of interest by CASEDAY
@linq times |>
by(:CASEDAY,
   MEANARRIVAL     = mean(value.(:ORARRIVAL)),
   SDARRIVAL       =  std(value.(:ORARRIVAL)),
   MEANINTAKE      = mean(value.(:ORINTAKE)),
   SDINTAKE        =  std(value.(:ORINTAKE)),
   MEANINDUCTION   = mean(value.(:INDUCTION)),
   SDINDUCTION     =  std(value.(:INDUCTION)),
   MEANPATPREP     = mean(value.(:PATIENTPREP)),
   SDPATPREP       =  std(value.(:PATIENTPREP)),
   MEANPREP        = mean(value.(:PREP)),
   SDPREP          =  std(value.(:PREP)),
   MEANEXTUBATION  = mean(value.(:EXTUBATION)),
   SDEXTUBATION    =  std(value.(:EXTUBATION)),
   MEANRECOVERYIN  = mean(value.(:RECOVERYIN)),
   SDRECOVERYIN    =  std(value.(:RECOVERYIN)),
   MEANRECOVERYOUT = mean(value.(:RECOVERYOUT)),
   SDRECOVERYOUT   =  std(value.(:RECOVERYOUT)))

# Summarize times of interest by PATIENTTYPE
@linq times |>
by(:PATIENTTYPE,
   MEANARRIVAL     = mean(value.(:ORARRIVAL)),
   SDARRIVAL       =  std(value.(:ORARRIVAL)),
   MEANINTAKE      = mean(value.(:ORINTAKE)),
   SDINTAKE        =  std(value.(:ORINTAKE)),
   MEANINDUCTION   = mean(value.(:INDUCTION)),
   SDINDUCTION     =  std(value.(:INDUCTION)),
   MEANPATPREP     = mean(value.(:PATIENTPREP)),
   SDPATPREP       =  std(value.(:PATIENTPREP)),
   MEANPREP        = mean(value.(:PREP)),
   SDPREP          =  std(value.(:PREP)),
   MEANEXTUBATION  = mean(value.(:EXTUBATION)),
   SDEXTUBATION    =  std(value.(:EXTUBATION)),
   MEANRECOVERYIN  = mean(value.(:RECOVERYIN)),
   SDRECOVERYIN    =  std(value.(:RECOVERYIN)),
   MEANRECOVERYOUT = mean(value.(:RECOVERYOUT)),
   SDRECOVERYOUT   =  std(value.(:RECOVERYOUT)))

# Summarize times of interest by OPERATINGROOMID
@linq times |>
by(:OPERATINGROOMID,
   MEANARRIVAL     = mean(value.(:ORARRIVAL)),
   SDARRIVAL       =  std(value.(:ORARRIVAL)),
   MEANINTAKE      = mean(value.(:ORINTAKE)),
   SDINTAKE        =  std(value.(:ORINTAKE)),
   MEANINDUCTION   = mean(value.(:INDUCTION)),
   SDINDUCTION     =  std(value.(:INDUCTION)),
   MEANPATPREP     = mean(value.(:PATIENTPREP)),
   SDPATPREP       =  std(value.(:PATIENTPREP)),
   MEANPREP        = mean(value.(:PREP)),
   SDPREP          =  std(value.(:PREP)),
   MEANEXTUBATION  = mean(value.(:EXTUBATION)),
   SDEXTUBATION    =  std(value.(:EXTUBATION)),
   MEANRECOVERYIN  = mean(value.(:RECOVERYIN)),
   SDRECOVERYIN    =  std(value.(:RECOVERYIN)),
   MEANRECOVERYOUT = mean(value.(:RECOVERYOUT)),
   SDRECOVERYOUT   =  std(value.(:RECOVERYOUT)))

# Summarize times of interest by PROCEDUREID
@linq times |>
by(:PROCEDUREID,
   MEANARRIVAL     = mean(value.(:ORARRIVAL)),
   SDARRIVAL       =  std(value.(:ORARRIVAL)),
   MEANINTAKE      = mean(value.(:ORINTAKE)),
   SDINTAKE        =  std(value.(:ORINTAKE)),
   MEANINDUCTION   = mean(value.(:INDUCTION)),
   SDINDUCTION     =  std(value.(:INDUCTION)),
   MEANPATPREP     = mean(value.(:PATIENTPREP)),
   SDPATPREP       =  std(value.(:PATIENTPREP)),
   MEANPREP        = mean(value.(:PREP)),
   SDPREP          =  std(value.(:PREP)),
   MEANEXTUBATION  = mean(value.(:EXTUBATION)),
   SDEXTUBATION    =  std(value.(:EXTUBATION)),
   MEANRECOVERYIN  = mean(value.(:RECOVERYIN)),
   SDRECOVERYIN    =  std(value.(:RECOVERYIN)),
   MEANRECOVERYOUT = mean(value.(:RECOVERYOUT)),
   SDRECOVERYOUT   =  std(value.(:RECOVERYOUT)))

# Calculate First Case Delay
const delay = Time(8) + Minute(15);
const delay_wednesday = Time(9) + Minute(15);
non_grand_round_days = @linq data |>
       where(Time.(:PATIENTARRIVEDDTTM) .> Time(7),
             Time.(:PATIENTARRIVEDDTTM) .< Time(8),
             # to account for emergency cases because they will not
             # be included in first case delay calculation
             :BOOKINGTYPEID .== "N",
             :CASEDAY .!= "Wednesday") |>
       transform(DELAY = convert.(Minute, delay - Time.(:PATIENTINDTTM))) |>
       select(:SURGEONID, :PROCEDUREID, :CASEDAY, :OPERATINGROOMID, :DELAY) |>
       where(value.(:DELAY) .> 0);

grand_round_days = @linq data |>
       where(Time.(:PATIENTARRIVEDDTTM) .> Time(8),
             Time.(:PATIENTARRIVEDDTTM) .< Time(9),
             # to account for emergency cases because they will not
             # be included in first case delay calculation
             :BOOKINGTYPEID .== "N",
             :CASEDAY .== "Wednesday") |>
       transform(DELAY = convert.(Minute, delay - Time.(:PATIENTINDTTM))) |>
       select(:SURGEONID, :PROCEDUREID, :CASEDAY, :OPERATINGROOMID, :DELAY) |>
       where(value.(:DELAY) .> 0);

# how to account for overnight case running till 8 AM
# ANSWER: such cases are not called, or called to a different OR

# Summarize first case delay by CASEDAY
@linq [non_grand_round_days; grand_round_days] |>
       by(:CASEDAY,
       MEANDELAY = mean(value.(:DELAY)),
       SDDELAY   =  std(value.(:DELAY)))

# Summarize first case delay by SURGEONID
@linq [non_grand_round_days; grand_round_days] |>
       by(:SURGEONID,
       MEANDELAY = mean(value.(:DELAY)),
       SDDELAY   =  std(value.(:DELAY)))

# Summarize first case delay by PROCEDUREID
@linq [non_grand_round_days; grand_round_days] |>
       by(:PROCEDUREID,
       MEANDELAY = mean(value.(:DELAY)),
       SDDELAY   =  std(value.(:DELAY)))

# Summarize first case delay by PATIENTTYPE
@linq [non_grand_round_days; grand_round_days] |>
       by(:PATIENTTYPE,
       MEANDELAY = mean(value.(:DELAY)),
       SDDELAY   =  std(value.(:DELAY)))

# Summarize first case delay by OPERATINGROOMID
@linq [non_grand_round_days; grand_round_days] |>
       by(:OPERATINGROOMID,
       MEANDELAY = mean(value.(:DELAY)),
       SDDELAY   =  std(value.(:DELAY)))

end # module
