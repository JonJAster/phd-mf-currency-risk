using Revise
using DataFrames
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_data()
    task_start = time()
    mf_data = _read_mf_timeseries()
    mf_info = _read_mf_crosssection()

    _drop_allmissing_funds!(mf_data, Not([:fundid, :secid, :date]); dims=:rows)

    printtime("initialising mutual fund data", task_start, minutes=false)
    return mf_data
end

function _read_mf_timeseries()
    
    mf_ret_tablename = "monthly_returns"
    mf_tna = _read_mf_tna(wrds)
    mf_fees = _read_mf_fees(wrds, force=true)

    close(wrds)
    
    mf_data = outerjoin(mf_ret, mf_tna; on=[:crsp_fundno, :caldt])
    mf_data[!, :exp_ratio] = Array{Union{Missing, Float64}}(missing, nrow(mf_data))

    for row in eachrow(mf_data)
        row_id = row.crsp_fundno
        row_date = row.caldt
        select_fees = mf_fees[mf_fees.crsp_fundno .== row_id, :]
        select_fees = select_fees[select_fees.begdt .<= row_date, :]
        select_fees = select_fees[select_fees.enddt .>= row_date, :]

        try
            (nrow(select_fees) > 0) && (row.exp_ratio = first(select_fees.exp_ratio))
        catch e
            println(first(select_fees.exp_ratio))
            error("stop")
        end
    end

    mf_header = _read_mf_header(wrds)

    mf_header

    # mf_fund_style = _read_mf_fund_style(wrds) TODO - activate this
    close(wrds)

    



    # folder = DIRS.mf.raw
    # files = readdir(folder, )
    # data = DataFrame[]
    # for file in files
    #     file in SKIP_FILES && continue

    #     filepath = joinpath(folder, file)
    #     fieldname = splitext(file)[1]
        
    #     data_part = init_raw(filepath)
    #     data_part_melt = stack(
    #         data_part, Not([:name, :fundid, :secid]), [:fundid, :secid],
    #         variable_name=:date, value_name=fieldname
    #     )

    #     data_part_melt.date = Date.(data_part_melt.date)

    #     push!(data, data_part_melt)
    # end
    return data
end

function _read_mf_ret(wrds; limit=nothing, force=false)
    file_root = "mf-ret"
    query = """
        select
            a.crsp_fundno,
            a.caldt,
            a.mret
        from crsp.monthly_returns a
    """

    data = _read_mf(wrds, query, file_root; limit=limit, force=force)
    return data
end

function _read_mf_tna(wrds; limit=nothing, force=false)
    file_root = "mf-tna"
    query = """
        select
            a.crsp_fundno,
            a.caldt,
            a.mtna
        from crsp.monthly_tna a
    """

    data = _read_mf(wrds, query, file_root; limit=limit, force=force)
    return data
end

function _read_mf_fees(wrds; limit=nothing, force=false)
    file_root = "mf-fees"
    query = """
        select
            a.crsp_fundno,
            a.begdt,
            a.enddt,
            a.exp_ratio,
            a.actual_12b1,
            a.max_12b1
        from crsp.fund_fees a
    """

    data = _read_mf(wrds, query, file_root; limit=limit, force=force)
    return data
end

function _read_mf_header(wrds; limit=nothing, force=false)
    file_root = "mf-header"
    query = """
        select
            a.crsp_fundno,
            a.crsp_cl_grp,
            a.fund_name,
            a.first_offer_dt,
            a.index_fund_flag,
            a.vau_fund,
            a.et_flag,
            a.dead_flag,
            a.delist_cd,
            a.merge_fundno
        from crsp.fund_hdr a
    """

    data = _read_mf(wrds, query, file_root; limit=limit, force=force)
    return data
end

function _read_mf_fund_style(wrds; limit=nothing, force=false)
    file_root = "mf-fund-style"
    query = """
        select
            a.crsp_fundno,
            a.begdt,
            a.enddt,
            a.crsp_obj_cd,
            a.si_obj_cd,

            a.weight
        from crsp.fund_style a
    """

    data = _read_mf(wrds, query, file_root; limit=limit, force=force)
    return data
end

function _read_mf(wrds, query, file_root; limit=nothing, force=false)
    """
    Reads mutual fund data from WRDS.

    Parameters
    ----------
    wrds : WRDS
        WRDS connection object.
    query : str
        SQL query to execute.
    file_root : str
        Root of the file name to save/load the data to/from disk.
    limit : Union{Nothing, Int}, optional
        Number of rows to read. If nothing, reads all rows.
    force : Bool, optional
        If true, forces the function to read from WRDS. If false, reads from
        the local file if it exists.
    """

    filepath = joinpath(DIRS.mf.raw, "$file_root.arrow")

    if !force && isfile(filepath)
        data = Arrow.Table(filepath) |> DataFrame
    else
        data = query_wrds(wrds, query; limit=limit, save_to=file_root)
    end

    return data
end

function _drop_allmissing_funds!(data)
    fund_level_missing_mask = combine(
        groupby(data, :fundid),
        [
            col => (x->all(ismissing, x)) => col*"_mask"
            for col in names(data[:, Not(:fundid, :secid, :date)])
        ]...
    )

    all_missing_funds = fund_level_missing_mask[
        all.(eachrow(fund_level_missing_mask[:, Not(:fundid)])), :fundid
    ]

    delete!(data, findall(in(all_missing_funds), data.fundid))
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = init_mf_data()
    output_filename = makepath(DIRS.mf.init, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund data", task_start, minutes=false)
end