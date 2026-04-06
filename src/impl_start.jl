using Printf

mutable struct Measurement
  min::Float64
  max::Float64
  sum::Float64
  amountOfPoints::Int64
end

function loadData()
  return readlines("./data/measurements.txt")
end

function processData(data)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  for point in data
    station, value = split(point, ";")
    valueAsFloat = parse(Float64, value)

    if haskey(dataPoints, station)
      existingPoint = get(dataPoints, station, 0)
      if existingPoint.min > valueAsFloat
        existingPoint.min = valueAsFloat
      end
      if existingPoint.max < valueAsFloat
        existingPoint.max = valueAsFloat
      end
      existingPoint.sum += valueAsFloat
      existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(valueAsFloat, valueAsFloat, valueAsFloat, 1))
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
