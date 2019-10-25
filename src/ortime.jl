module ortime

using Revise;

using CSV;
using DataFrames;
using CategoricalArrays;
using Dates;
using StatsKit;

import Dates.Time;

include("src/functions.jl")

# Read the Data
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

times = DataFrame(
                  DAYOFTHEWEEK = String[],
                  LOCATIONID = String[],
                  OPERATINGROOMID = String[],
                  PATIENTTYPE = String[],
                  PROCEDUREID = String[],
                  SURGEONID = String[],
                  ORARRIVAL = Dates.Minute[],
                  ORINTAKE = Dates.Minute[],
                  INDUCTION = Dates.Minute[],
                  PREP = Dates.Minute[],
                  EXTUBATION = Dates.Minute[]
#                 TIMEOUT = Dates.Minute[],
#                 RECOVERYIN = Dates.Minute[],
#                 RECOVERYOUT = Dates.Minute[]
                 );

for a in (data.PATIENTARRIVEDDTTM - data.PATIENTSENTDTTM)
    push!(times.ORARRIVAL, convert(Dates.Minute, a))
end

for a in (data.PATIENTINDTTM - data.PATIENTARRIVEDDTTM)
    push!(times.ORINTAKE, convert(Dates.Minute, a))
end

for a in (data.PATIENTREADYFORDTTM - data.PATIENTINDTTM)
    push!(times.INDUCTION, convert(Dates.Minute, a))
end

#for a in (data.PREPSTARTDTTM - data.PATIENTINDTTM)
#    push!(times.PATIENTPREP, convert(Dates.Minute, a))
#end

for a in (data.INCISIONDTTM - data.PREPSTARTDTTM)
    push!(times.PREP, convert(Dates.Minute, a))
end

for a in (data.TIMEOUTDTTM - data.DRESSINGDTTM )
    push!(times.EXTUBATION, convert(Dates.Minute, a))
end

#for a in (data.RECOVERYINDTTM - data.TIMEOUTDTTM )
#    push!(times.RECOVERYIN, convert(Dates.Minute, a))
#end
#
#for a in (data.RECOVERYOUTDTTM - data.RECOVERYINDTTM)
#    push!(times.RECOVERYOUT, convert(Dates.Minute, a))
#end

for a in data.CASEDATE
    push!(times.DAYOFTHEWEEK, Dates.dayname(a))
end

times.LOCATIONID      = data.LOCATIONID;
times.OPERATINGROOMID = data.OPERATINGROOMID;
times.PATIENTTYPE     = data.PATIENTTYPE;
times.PROCEDUREID     = data.PROCEDUREID;
times.SURGEONID       = data.SURGEONID;

times = categorical(times, [:DAYOFTHEWEEK, :LOCATIONID,
                            :OPERATINGROOMID, :PATIENTTYPE,
                            :PROCEDUREID, :SURGEONID]);
levels!(times.DAYOFTHEWEEK, ["Monday","Tuesday","Wednesday",
                            "Thursday","Friday","Saturday","Sunday"]);


by(times, :DAYOFTHEWEEK,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :DAYOFTHEWEEK,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

by(times, :LOCATIONID,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :LOCATIONID,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

by(times, :OPERATINGROOMID,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :OPERATINGROOMID,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

by(times, :PROCEDUREID,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :PROCEDUREID,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

by(times, :PATIENTTYPE,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :PATIENTTYPE,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

by(times, :SURGEONID,
   MEANARRIVAL      = :ORARRIVAL    => meantime,
   MEANINTAKE       = :ORINTAKE     => meantime,
   MEANREADY        = :INDUCTION    => meantime,
   MEANPREP         = :PREP         => meantime,
   MEANINCISION     = :EXTUBATION   => meantime)

by(times, :SURGEONID,
   SDARRIVAL      = :ORARRIVAL    => stdtime,
   SDINTAKE       = :ORINTAKE     => stdtime,
   SDREADY        = :INDUCTION    => stdtime,
   SDPREP         = :PREP         => stdtime,
   SDINCISION     = :EXTUBATION   => stdtime)

# Find First Case Delay
@linq data |>
       where(Time.(:PATIENTARRIVEDDTTM) .> Time(7), Time.(:PATIENTARRIVEDDTTM) .< Time(8), :BOOKINGTYPEID .== "N", Dates.dayname.(:CASEDATE) .!= "Wednesday") |>
       select(:CASEDATE, :OPERATINGROOMID, :MRNUMBER, :PATIENTSENTDTTM, :PATIENTARRIVEDDTTM, :PATIENTINDTTM) |>
       unique(:MRNUMBER) |>
       transform(CASEDAY = Dates.dayname.(:CASEDATE)) |>
       groupby(:CASEDATE)
end # module
