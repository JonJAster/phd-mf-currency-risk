using FromFile
using DataFrames
using CSV
using Dates

@from "../../utils.jl" using ProjectUtilities

const EXPLICIT_TYPES = Dict(:FundId => String15, :SecId => String15)
const ID_COLS = [:name, :fundid, :secid]

function main()
    startedat = time()

    info = loadinfo()
    country_group_map = map_countrygroups()

    info.country_group .= get.(Ref(country_group_map), info.domicile, "other")
    secid_group_map = Dict(info.secid, info.country_group)
    
    println("Regrouping files...")
    for folder in FUND_DATA_FOLDERS
        folder_startedat = time()

        data = loaddata(folder)

        savegroups(folder, data; mapping=secid_group_map)

        printtime(
            "saving raw mutual fund data by country group", startedat;
            process_subtask=folder, process_starttime=folder_startedat, minutes=true
        )
    end

    savegroups(info, "info"; mapping=secid_group_map)

    printtime("regrouping all raw mutual fund data", startedat)
    return nothing
end

function loadinfo()
    info = qload(
        PATHS.rawfunds, "info";
        types=EXPLICIT_TYPES, stringtype=String, truestring=["Yes"], falsestrings=["No"]
    )
    normalise_headings!(info; infodata=true)
    return nothing
end

function map_countrygroups()
    group_names = keys(COUNTRY_GROUPS)
    countries_in_group = values(COUNTRY_GROUPS)
    country_group_pairs = Iterators.flatten(
        [list .=> name for (name, list) in zip(group_names, countries_in_group)]
    )

    groupmap = Dict(country_group_pairs)
    return groupmap
end

function loaddata(folder)
    data = qload(PATHS.rawfunds, folder; types=EXPLICIT_TYPES, stringtype=String)
    normalise_headings!(data)

    dropempty!(data, [:fundid, :secid])
    dropempty!(data; how=:all, dims=:both)
    parsecommas!(data, Not(ID_COLS))

    return data
end

function savegroups(folder, data; mapping)
    data.group = get.(Ref(mapping), data.secid, "other")
    groupby(data, :group) do group, group_data
        qsave(PATHS.groupedfunds, "$folder.csv", group; data=group_data)
    end
end
    
function load_file_by_parts(folder)
    folderstring = joinpath(INPUT_DIR, folder)
    files = readdir(folderstring)
    data_parts = DataFrame[]

    for file in files
        filestring = joinpath(folderstring, file)
        
        # if folder == "info"
        #     read_data = CSV.read(
        #         filestring, DataFrame, types=EXPLICIT_TYPES, stringtype=String,
        #         truestrings=["Yes"], falsestrings=["No"]
        #     )

        #     normalise_headings!(read_data, infodata=true)
        else
            read_data = CSV.read(
                filestring, DataFrame, types=EXPLICIT_TYPES, stringtype=String,
            )

            normalise_headings!(read_data)

            dropmissing!(read_data, [:fundid, :secid])
            read_data = (fix_thousands_commas ∘ drop_blank_ids ∘ drop_empty_rows)(read_data)
        end

        push!(data_parts, read_data)
    end

    data = vcat(data_parts...) |> drop_empty_cols
    return data
end

function drop_blank_ids(df)
    return df[.!ismissing_or_blank.(df.fundid) .&& .!ismissing_or_blank.(df.secid), :]
end

function normalise_headings!(data_part; infodata=false)
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
        column_names = [ID_COLS; Dates.format.(data_dates, "yyyy-mm")]
        rename!(data_part, column_names)
    end
end

function fix_thousands_commas(df)
    col_types = map(col -> eltype(col), eachcol(df[!, Not(ID_COLS)]))
    isstring(T) = T <: Union{Missing, AbstractString} && T != Missing
    isnumber(T) = T <: Union{Missing, Number} && T != Missing
    data_contains_strings = any(isstring, col_types)
    data_contains_numbers= any(isnumber, col_types)

    mistyped_cols = Int[]
    if data_contains_strings && data_contains_numbers
        mistyped_cols = findall(isstring, col_types) .+ length(ID_COLS)
    elseif data_contains_strings
        data_query_idx = findfirst(!ismissing_or_blank, df[!, end])
        data_query = df[data_query_idx, end]
        valid_number = !occursin(r"[^\d.,]", data_query)
        valid_number && (mistyped_cols = (1:length(col_types))  .+ length(ID_COLS))
    end

    isempty(mistyped_cols) && return df
    return strip_thousands_commas(df, mistyped_cols)
end

function strip_thousands_commas(df, mistyped_cols)
    strip_commas(x) = ismissing(x) ? missing : replace(x, ","=>"")

    for col in mistyped_cols
        df[!, col] = strip_commas.(df[!, col])
    end

    return df
end

function drop_empty_rows(df)
    where_data_exists = .!ismissing_or_blank.(df[!, Not(ID_COLS)])
    any_data = reduce(.|, eachcol(where_data_exists))
    return df[any_data, :]
end

function drop_empty_cols(df)
    where_data_exists = .!ismissing_or_blank.(df)
    any_data = any.(eachcol(where_data_exists))
    return df[:, any_data]
end

function save_file_by_country_group(data, folder, group_map)
    for group in [keys(COUNTRY_GROUPS)..., "other"]
        # Split the dataframe into a subset of rows with the given country group
        data_split = data[map(secid -> get(group_map, secid, ""),
                          data[!, :secid]) .== group, :]
        
        output_filestring = makepath(OUTPUT_DIR, folder, "mf_$(folder)_$group.csv")

        CSV.write(output_filestring, data_split)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

function printtime2(
    task::AbstractString, start_time;
    process_subtask=nothing
)
    nothing
end