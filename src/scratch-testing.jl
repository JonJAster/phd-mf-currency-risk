using Revise
using BenchmarkTools
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    filepaths = readdir(DIRS.mf.raw, join=true)

    info = CSV.read(joinpath(DIRS.mf.raw, "info.csv"), DataFrame)

    info_fundids = info.FundId |> Set
    info_secids = info.SecId |> Set

    raw_data = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)

    fundid_raw_data = combine(
        groupby(raw_data, :FundId),
        propertynames(raw_data[:, Not(:FundId, :SecId, :Name)]) .=> (x->all(!ismissing, x))
    )

    count(==(true), fundid_raw_data[:, Not(:FundId)] |> Matrix)
    raw_fundids = raw_data.FundId |> Set

    raw_data.any_gret = any.(.!ismissing.(eachrow(raw_data[:, Not(:Name, :FundId, :SecId)])))
    fund_raw_data_mask = combine(
        groupby(raw_data, :FundId),
        propertynames(raw_data[:, Not(:FundId, :SecId, :Name)]) .=> (x->all(!ismissing, x))
    )

    fund_raw_data_mask.any_gret = any.(.!ismissing.(eachrow(fund_raw_data_mask[:, Not(:FundId)])))
    count(==(1), fund_raw_data_mask.any_gret)

    ###

    data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))

    data[data.fundid .== "FS00009GK0" .&& data.date .== Date(2019,1,1), :]
    count_obs(data, :gross_returns)

    data_fundid = combine(
        groupby(data, :fundid),
        :gross_returns => (x->all(!ismissing, x)) => :all_valid
    )

    data_fundids = data.fundid |> Set

    diffids = setdiff(raw_fundids, data_fundids)

    raw_fundids = raw_data[raw_data.FundId .∈ Ref(diffids),:]

    count(!ismissing, raw_fundids[:, Not(:Name, :FundId, :SecId)] |> Matrix)

    ###

    fundid_data = combine(
        groupby(data, [:fundid, :date]),
        :gross_returns => mean => :mean_gret
    )

    unique_init_fund_gret = fundid_data.mean_gret |> Set
    
    fundid_raw_data = combine(
        groupby(raw_data, :FundId),
        propertynames(raw_data[:, Not(:Name, :FundId, :SecId)]) .=> mean
    )

    unique_raw_fund_gret = fundid_raw_data[:, Not(:FundId)] |> Matrix |> Set

    unique_init_fund_gret
    unique_raw_fund_gret

    for i in unique_init_fund_gret
        if !(i in unique_raw_fund_gret)
            error(i)
        end
    end

    4.934068571428571 in unique_init_fund_gret

    fundid_data[coalesce.(fundid_data.mean_gret .== 4.934068571428571, false), :]

    raw_data[raw_data.FundId .== "FS00009GK0", r"Name|FundId|SecId|.*2019-01.*"]

    count(!ismissing, fundid_data[!, :mean_gret])

    ### 

    data[data.fundid .== "FS00009GK0" .&& data.date .== Date(2019,2,1), :]
    
end

function count_obs_local(df, count_col; filter_down=false) # df = copy(data); count_col = :gross_returns
    if :secid in propertynames(df)
        valid_secids = combine(
            groupby(df, :secid),
            count_col => (x->any(!ismissing, x)) => :any_valid
        )
        valid_secids = valid_secids[valid_secids.any_valid, :secid] |> Set
        valid_secid_data = df[df.secid .∈ Ref(valid_secids), :]

        df_fundid = combine(
            groupby(df, [:fundid, :date]),
            count_col => (x->all(!ismissing, x)) => :all_valid_on_date
        )

        valid_fundids = combine(
            groupby(df_fundid, :fundid),
            :all_valid_on_date => any => :any_valid
        )

        # df[df.fundid .== "FS0000BK7C", :]

        valid_fundids = valid_fundids[valid_fundids.any_valid, :fundid] |> Set
        valid_fundid_mask = df_fundid[df_fundid.fundid .∈ Ref(valid_fundids), :]

        output = (
            unique_secids = length(valid_secids),
            unique_fundids = length(valid_fundids),
            class_month_obs = count(!ismissing, valid_secid_data[!, count_col]),
            fund_month_obs = count(x->x, valid_fundid_mask[!, :all_valid_on_date])
        )

        return output
    end
end