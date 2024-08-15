using Revise
using BenchmarkTools
using DataFrames
using CSV
using REPL
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using StatsModels
using Plots
using Distributions
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function test()
    rets_filename_usa = joinpath(DIRS.combo.weighted, "ff_usa_ffc6.arrow")
    rets_filename_dev = joinpath(DIRS.combo.weighted, "ff_dev_ffc6.arrow")

    rets_usa = loadarrow(rets_filename_usa)
    rets_dev = loadarrow(rets_filename_dev)

    rets_data_usa = innerjoin(mf_data, rets_usa, on = [:fundid, :date])
    rets_data_dev = innerjoin(mf_data, rets_dev, on = [:fundid, :date])

    rets_data_usa_f = filter(x->x.foreign, rets_data_usa)
    rets_data_usa_d = filter(x->!x.foreign, rets_data_usa)

    mean(rets_data_usa_d.ret_alpha_m1 .* 100)

    rets_data_dev_f = filter(x->x.foreign, rets_data_dev)
    rets_data_dev_d = filter(x->!x.foreign, rets_data_dev)

    rets_data_usa_f_1 = filter(x->x.date < Date(2011), rets_data_usa_f)
    rets_data_usa_f_2 = filter(x->x.date >= Date(2011), rets_data_usa_f)

    rets_data_usa_d_1 = filter(x->x.date < Date(2011), rets_data_usa_d)
    rets_data_usa_d_2 = filter(x->x.date >= Date(2011), rets_data_usa_d)

    rets_data_dev_f_1 = filter(x->x.date < Date(2011), rets_data_dev_f)
    rets_data_dev_f_2 = filter(x->x.date >= Date(2011), rets_data_dev_f)

    rets_data_dev_d_1 = filter(x->x.date < Date(2011), rets_data_dev_d)
    rets_data_dev_d_2 = filter(x->x.date >= Date(2011), rets_data_dev_d)

    f_cols = [:ret_m1, :ret_alpha_m1, :ret_mkt_m1, :ret_smb_m1, :ret_hml_m1, :ret_rmw_m1, :ret_cma_m1, :ret_wml_m1]
    println("Full Domestic USA")
    df_mean = DataFrame(f_cols .=> round.(mean.(eachcol(rets_data_usa_d[!, f_cols] .* 100)), digits=2))
    df_median = DataFrame(f_cols .=> round.(median.(eachcol(rets_data_usa_d[!, f_cols] .* 100)), digits=2))
    df_std = DataFrame(f_cols .=> round.(std.(eachcol(rets_data_usa_d[!, f_cols] .* 100)), digits=2))
    df_stats = hcat(DataFrame(:stat => [:mean, :median, :std]), vcat(df_mean, df_median, df_std))
    x = describe(rets_data_usa_d[!, f_cols] .* 100)
    sum(x[2:8, :mean])
    println("Early Domestic USA")
    println(describe(rets_data_usa_d_1[!, f_cols] .* 100))
    println("Late Domestic USA")
    println(describe(rets_data_usa_d_2[!, f_cols] .* 100))
    println("Full Domestic DEV")
    println(describe(rets_data_dev_d[!, f_cols] .* 100))
    println("Early Domestic DEV")
    println(describe(rets_data_dev_d_1[!, f_cols] .* 100))
    println("Late Domestic DEV")
    println(describe(rets_data_dev_d_2[!, f_cols] .* 100))
    println("Full Foreign USA")
    println(describe(rets_data_usa_f[!, f_cols] .* 100))
    println("Early Foreign USA")
    println(describe(rets_data_usa_f_1[!, f_cols] .* 100))
    println("Late Foreign USA")
    println(describe(rets_data_usa_f_2[!, f_cols] .* 100))
    println("Full Foreign DEV")
    println(describe(rets_data_dev_f[!, f_cols] .* 100))
    println("Early Foreign DEV")
    println(describe(rets_data_dev_f_1[!, f_cols] .* 100))
    println("Late Foreign DEV")
    println(describe(rets_data_dev_f_2[!, f_cols] .* 100))
    
    
    end




    beta_filename = joinpath(DIRS.combo.return_betas, "ff_dev_ffc6.arrow")
    mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    beta_data = loadarrow(beta_filename)
    mf_data = loadarrow(mf_filename)

    betas = unstack(beta_data, [:fundid, :date], :factor, :coef)
    dropmissing!(betas)

    data = innerjoin(mf_data, betas, on = [:fundid, :date])

    data_f = filter(x->x.foreign, data)
    data_d = filter(x->!x.foreign, data)

    data_f_1 = filter(x->x.date < Date(2011), data_f)
    data_f_2 = filter(x->x.date >= Date(2011), data_f)

    data_d_1 = filter(x->x.date < Date(2011), data_d)
    data_d_2 = filter(x->x.date >= Date(2011), data_d)

    for f in [:mkt, :smb, :hml, :rmw, :cma, :wml]
        println(f)
        println("Early Domestic")
        println(round.(percentile(data_d_1[!, f], [0, 1, 25, 50, 75, 99, 100]) ./ .17, digits=2) )
        println("Late Domestic")
        println(round.(percentile(data_d_2[!, f], [0, 1, 25, 50, 75, 99, 100]) ./ .12, digits=2) )
        println("Early Foreign")
        println(round.(percentile(data_f_1[!, f], [0, 1, 25, 50, 75, 99, 100]) ./ .22, digits=2))
        println("Late Foreign")
        println(round.(percentile(data_f_2[!, f], [0, 1, 25, 50, 75, 99, 100]) ./ .21, digits=2))
        println()
    end



    weighted.sum = weighted[!, 4] .+ weighted[!, 5] .+ weighted[!, 6] .+ weighted[!, 7] .+ weighted[!, 8]
    testdf = DataFrame(a = repeat([1,2], 5), b = 1:10, c = 1:10)
    x = combine(
        groupby(testdf, :a),
        :b => sum => :bsum,
        [:b, :c] => ((x,y)->x+y) => :bplusc,
        [:b, :c] => ((x,y)->[mean(x), mean(y)])# .=> [:bmu, :cmu])
        #:c => (x->sum(x), x->mean(x)) => [:csum, :cmean]
    )
end