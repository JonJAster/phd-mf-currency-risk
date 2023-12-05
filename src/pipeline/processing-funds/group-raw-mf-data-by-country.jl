using FromFile
using DataFrames
using CSV
using Arrow
using Dates

@from "../../utils.jl" using ProjectUtilities

const EXPLICIT_TYPES = Dict(:FundId => String15, :SecId => String15)
const ID_COLS = [:name, :fundid, :secid]

function main()
    startedat = time()

    info = _loadinfo()
    country_group_map = _map_countrygroups()

    info.country_group .= get.(Ref(country_group_map), info.domicile, "other")
    secid_group_map = Dict(info.secid .=> info.country_group)
    
    println("Regrouping files...")
    for folder in values(FUND_FIELDS)
        folder_startedat = time()

        data = _loaddata(folder)
        
        _savegroups(folder, data; mapping=secid_group_map)

        printtime(
            "saving raw mutual fund data by country group", startedat;
            process_subtask=folder, process_start_time=folder_startedat
        )
    end

    _savegroups("info", info; mapping=secid_group_map)

    printtime("regrouping all raw mutual fund data", startedat, minutes=true)
    return nothing
end

function _loadinfo()
    info = qload(
        PATHS.rawfunds, "info";
        types=EXPLICIT_TYPES, stringtype=String, truestring=["Yes"], falsestrings=["No"]
    )
    _normalise_headings!(info; infodata=true)
    return info
end

function _map_countrygroups()
    group_names = keys(COUNTRY_GROUPS)
    countries_in_group = values(COUNTRY_GROUPS)
    country_group_pairs = Iterators.flatten(
        [list .=> name for (name, list) in zip(group_names, countries_in_group)]
    )

    groupmap = Dict(country_group_pairs)
    return groupmap
end

function _loaddata(folder)
    folderpath = qpath(PATHS.rawfunds, "$folder/")

    data_parts = DataFrame[]
    for filepath in readdir(folderpath; join=true)
        
        data_part = CSV.read(
            filepath, DataFrame;
            types=EXPLICIT_TYPES, stringtype=String, groupmark=',', rows_to_check=200
        )

        _normalise_headings!(data_part)

        _drop_incompleteids!(data_part)
        _drop_emptyrows!(data_part)

        push!(data_parts, data_part)
    end

    data = vcat(data_parts...)
    _drop_emptycols!(data)

    return data
end

function _savegroups(folder, data; mapping)
    data.group = get.(Ref(mapping), data.secid, "other")
    gdf = groupby(data, :group)
    for (group_key, group_data) in pairs(gdf)
        group_name = group_key.group
        qsave(PATHS.groupedfunds, folder, "$group_name.csv"; data=group_data)
    end
end

function _normalise_headings!(data_part; infodata=false)
    if infodata
        raw_names = names(data_part)
        re_underscored_chars = r"[\s\(\)\-]+(?!$)"
        re_erased_chars = r"[\s\(\)\-]+$"

        underscored_names = replace.(raw_names, re_underscored_chars => "_")
        tailclipped_names = replace.(underscored_names, re_erased_chars => "")
        normalised_names = replace.(tailclipped_names, r"&" => "and")

        rename!(data_part, lowercase.(normalised_names))
    else
        datestring = r"\d{4}-\d{2}"
        n_idcols = length(ID_COLS)
        n_datacols = ncol(data_part) - n_idcols
        first_datacol = n_idcols + 1
        
        start_date = match(datestring, names(data_part)[first_datacol]).match
        data_dates = [Date(start_date) + Month(i) for i in 0:(n_datacols-1)]
        date_names = Dates.format.(data_dates, "yyyy-mm") .|> Symbol
        column_names = [ID_COLS; date_names]
        rename!(data_part, column_names)
    end
end

function _drop_incompleteids!(df)
    incomplete_mask = any(_lostdata(df, [:fundid, :secid]), dims=2) |> vec
    incomplete_rows = (1:nrow(df))[incomplete_mask]
    deleteat!(df, incomplete_rows)
end

function _drop_emptycols!(df)
    empty_mask = all(_lostdata(df, Not(ID_COLS)), dims=1) |> vec
    empty_cols = names(df[!, Not(ID_COLS)])[empty_mask]
    select!(df, Not(empty_cols))
end

function _drop_emptyrows!(df)
    empty_mask = all(_lostdata(df, Not(ID_COLS)), dims=2) |> vec
    empty_rows = (1:nrow(df))[empty_mask]
    deleteat!(df, empty_rows)
end

function _lostdata(df, cols)
    islost = BitMatrix(undef, size(df[!, cols]))
    checkcols = eachcol(df[!, cols])
    for (i, col) in enumerate(checkcols)
        islost[:, i] .= ismissing.(col) .| isequal.(col, "")
    end
    return islost
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end