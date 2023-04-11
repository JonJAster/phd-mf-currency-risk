using CSV
using DataFrames
using ShiftedArrays: lead, lag

const INPUT_FILESTRING_BASE = "./data/cleaned/mutual-funds/regrouped"
const OUTPUT_FILESTRING_BASE = "./data/prepared/mutual-funds"

const ID_COLUMNS = [:name, :fundid, :secid]

const COUNTRY_GROUPS = [
    "lux", "kor", "usa", "can-chn-jpn", "irl-bra",
    "gbr-fra-ind", "esp-tha-aus-zaf-mex-aut-che"
]

const FIELD_FOLDERS = [
    "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

function load_dataset(country_group)
    info_filestring = joinpath(INPUT_FILESTRING_BASE, "info", "mf_info_$country_group.csv")
    mf_info = CSV.read(info_filestring, DataFrame)

    data_field_set = DataFrame[]
    for folder in FIELD_FOLDERS
        push_data_part!(data_field_set, folder, country_group)
    end

    global testdfs = data_field_set

    mf_data = reduce((x,y) -> outerjoin(x,y,on=[ID_COLUMNS..., :date]), data_field_set)

    return mf_data
end

function push_data_part!(data_field_set, folder, country_group)
    filestring = joinpath(INPUT_FILESTRING_BASE, folder, "mf_$(folder)_$country_group.csv")
    data_field = CSV.read(filestring, DataFrame)

    if folder == "info"
        push!(data_field_set, data_field)
    else
        stacked_data = (
            stack(data_field, Not(ID_COLUMNS), variable_name=:date, value_name=folder)
        )
        push!(data_field_set, stacked_data)
    end
end
    


function main()
    x = load_dataset("other")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

# test for any missing values in any id columns for each of the field FIELD_FOLDERS
function test_missing_values()
    for folder in FIELD_FOLDERS
        for country_group in COUNTRY_GROUPS
            filestring = joinpath(INPUT_FILESTRING_BASE, folder, "mf_$(folder)_$country_group.csv")
            data_field = CSV.read(filestring, DataFrame)
            for id_column in ID_COLUMNS
                if any(ismissing, data_field[!, id_column])
                    println("Missing values in $id_column for $folder in $country_group")
                end
            end
        end
    end
end

test_missing_values()

# in each of the 7 dataframes held in testdfs, check for missing values in any of the first four columns
for (i,df) in enumerate(testdfs)
    missing_values = ismissing.(df[:, ID_COLUMNS])

    for (j, col) in enumerate(eachcol(missing_values))
        if any(col)
            println("Missing values in column $j in dataframe $i")
        end
    end
end

findfirst(ismissing, first(testdfs).fundid)

testdfs[1][1520, :]

testfolder = joinpath(INPUT_FILESTRING_BASE, "local-monthly-gross-returns", "mf_local-monthly-gross-returns_other.csv")

testdf = CSV.read(testfolder, DataFrame)

findfirst(ismissing, testdf.fundid)

testdf[1520, :]