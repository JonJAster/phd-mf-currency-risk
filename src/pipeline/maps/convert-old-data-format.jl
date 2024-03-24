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
    convert_equity_data()
end

function convert_equity_data()
    old_filename_equities = "old_global_equity_factors.arrow"
    old_filename_currencies = "old_currency_factors.arrow"
    new_filename = "factors.arrow"

    old_filepath_eq = joinpath(OLD_DIRPATH, old_filename_equities)
    old_filepath_cur = joinpath(OLD_DIRPATH, old_filename_currencies)
    new_filepath = joinpath(BACKUP_DIRPATH, new_filename)

    old_data_eq = loadarrow(old_filepath_eq)
    old_data_cur = loadarrow(old_filepath_cur)
    testview = loadarrow(joinpath(DIRS.combo.factors, "factors.arrow"))

    first(old_data_eq,5)
    first(testview,5)

    new_eq_data = stack(old_data_eq, Not(:date), :date, variable_name=:factor, value_name=:ret)
    new_eq_data.region .= "DEV" # The equity factors aren't actually dev, they're world.
    new_cur_data = stack(old_data_cur, Not(:date), :date, variable_name=:factor, value_name=:ret)
    new_cur_data.region .= "FX"

    new_data = vcat(new_eq_data, new_cur_data)
    select!(new_data, [:region, :date, :factor, :ret])
    
    
    new_data.date = firstdayofmonth.(new_data.date)
    new_data.ret ./= 100

    Arrow.write(new_filepath, new_data)
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
