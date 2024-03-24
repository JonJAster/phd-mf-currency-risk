using Revise
using DataFrames
using Arrow
using CSV
using Dates
using ShiftedArrays: lead, lag

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

const OLD_DIRPATH = joinpath(DIRS.test, "old-comparison-data/old-format")
const NEW_DIRPATH = joinpath(DIRS.test, "old-comparison-data/new-format")
const BACKUP_DIRPATH = joinpath(DIRS.test, "old-comparison-data/new-format/magnitude-adjusted")

function main()
    convert_excess_fund_data()
end

function convert_excess_fund_data()
    old_filename="old_excess_fund_data.arrow"
    new_filename="mf-data.arrow"

    old_filepath = joinpath(OLD_DIRPATH, old_filename)
    new_filepath = joinpath(NEW_DIRPATH, new_filename)
    backup_filepath = joinpath(BACKUP_DIRPATH, new_filename)

    old_data = loadarrow(old_filepath)
    new_data = copy(old_data)
    new_data.date = firstdayofmonth.(old_data.date)
    new_data.net_assets_m1 = lag(old_data.fund_assets)
    rename!(
        new_data,
        :fund_flow => :flow,
        :mean_costs => :costs,
    )
    
    select!(new_data, [:fundid, :date, :flow, :ex_ret, :costs, :net_assets_m1])

    Arrow.write(new_filepath, new_data)

    new_data[!, [:flow, :ex_ret, :costs]] ./= 100
    Arrow.write(backup_filepath, new_data)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
