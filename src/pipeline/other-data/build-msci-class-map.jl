using Revise
using DataFrames
using CSV
using Dates

include("../../shared/CommonConstants.jl")
include("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function build_msci_class_map()
    task_start = time()
    current_filepath = joinpath(DIRS.map.raw, "msci-class-current.csv")
    changes_filepath = joinpath(DIRS.map.raw, "msci-class-changes.csv")

    msci_class_map = CSV.read(current_filepath, DataFrame)
    class_changes = CSV.read(changes_filepath, DataFrame)

    _populate_historical_classes!(msci_class_map, class_changes)

    printtime("building MSCI class map", task_start, minutes=false)
    return msci_class_map
end

function _populate_historical_classes!(msci_class_map, class_changes)
    @assert length(unique(msci_class_map.date)) == 1
    penultimate_date = first(msci_class_map.date) - Month(1)
    earliest_date = Date(1990, 1, 1)

    for date in penultimate_date:Month(-1):earliest_date
        _generate_prior_classes!(msci_class_map, class_changes, date)
    end
end

function _generate_prior_classes!(msci_class_map, class_changes, date)
    classes_to_copy = _carry_down_classes(msci_class_map, date)
    length(class_changes) > 0 && _apply_changes!(classes_to_copy, class_changes)
    append!(msci_class_map, classes_to_copy)
    return
end

function _carry_down_classes(msci_class_map, date)
    following_date = date + Month(1)
    msci_class_map[msci_class_map.date .== following_date, :]

    classes_to_copy.date .-= Month(1)

    return classes_to_copy
end

function _apply_changes!(classes_to_copy, class_changes)
    for change_instruction in eachrow(class_changes)
        target_country = change_instruction.country_code
        target_class = change_instruction.change_from
        class_before_change = change_instruction.change_from
        target_row = (classes_to_copy.country_code .== target_country)
        _assert_valid_change(classes_to_copy[target_row, :], class_before_change)

        if ismissing(change_instruction.change_to)
            append!(
                classes_to_copy,
                DataFrame(
                    country_code = target_country,
                    date = change_instruction.date,
                    class = target_class,
                )
            )
        elseif ismissing(target_class)
            drop!(classes_to_copy, target_row)
        else
            classes_to_copy[target_row, :msci_class] .= target_class
        end
    end

    return
end

function _assert_valid_change(copied_row, class_before_change)
    if ismissing(class_before_change)
        @assert nrow(copied_row) == 0
    else
        @assert first(copied_row.msci_class) == class_before_change
    end
    
    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = build_msci_class_map()
    output_filename = makepath(DIRS.map.refined, "msci-class.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing MSCI class map", task_start, minutes=false)
end