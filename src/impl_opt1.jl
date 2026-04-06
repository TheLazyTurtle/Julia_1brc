using Printf

mutable struct Measurement
  min::Float64
  max::Float64
  sum::Float64
  amountOfPoints::Int64
end

function loadData()
  lines = Vector{String}(undef, 10_000_000)
  open("./data/measurements.txt") do io
    i = 1
    for line in eachline(io)
      lines[i] = line
      i += 1
    end
  end

  return lines
end

function efficientSplit(line::String, delim::Char)
  i = findfirst(==(delim), line)
  @inbounds return SubString(line, firstindex(line), prevind(line, i)), parse(Float64, SubString(line, nextind(line, i), lastindex(line)))
end

function processData(data)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  for point in data
    passedSeperator = false

    station, value = efficientSplit(point, ';')

    if haskey(dataPoints, station)
      existingPoint = get(dataPoints, station, 0)
      if existingPoint.min > value
        existingPoint.min = value
      end
      if existingPoint.max < value
        existingPoint.max = value
      end
      existingPoint.sum += value
      existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(value, value, value, 1))
    end
  end

  return dataPoints
end

function stortData!(dataPoints)
  sort!(collect(dataPoints), by = x -> x[1])
end

function printData(dataPoints)
  print("{")
  for (station, model) in dataPoints
    @printf "%s=%.1f/%.1f/%.1f, " station model.min (model.sum/model.amountOfPoints) model.max
  end
  print("}")
end

function __init__()
  data = loadData()
  dataPoints = processData(data)
  stortData!(dataPoints)
  printData(dataPoints)
end

__init__()
sleep(1)
