module Solution
export solve

using Base.Threads

const amountOfLines = 100_000_000
testLock = ReentrantLock()

mutable struct Measurement
  @atomic min::Float32
  @atomic max::Float32
  @atomic sum::Float32
  @atomic amountOfPoints::Int64
end

function fastParseFloat(s::SubString{String})
  @inbounds begin
    data = codeunits(s)
    i = 1

    neg = data[i] == 0x2d  # '-'
    i += neg

    d1 = data[i] - 0x30
    i += 1

    if data[i] == 0x2e  # '.'
        frac = data[i+1] - 0x30
        val = d1 + 0.1f0 * frac
        return neg ? -val : val
    else
        d2 = data[i] - 0x30
        i += 1
        frac = data[i+1] - 0x30
        val = (10*d1 + d2) + 0.1f0 * frac
        return neg ? -val : val
    end
  end
end

function efficientSplit(line::String, delim::Char)
  i = findfirst(==(delim), line)
  @inbounds begin
    name = SubString(line, firstindex(line), prevind(line, i))
    val = fastParseFloat(SubString(line, nextind(line, i), lastindex(line)))
    return name, val
  end
end

function processData(data::SubArray{String})
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  @inbounds for point in data
    station, value = efficientSplit(point, ';')

    existingPoint = get(dataPoints, station) do
      nothing
    end
    if existingPoint != nothing 
      @atomic min(existingPoint.min, value)
      @atomic max(existingPoint.max, value)
      @atomic existingPoint.sum += value
      @atomic existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(value, value, value, 1))
    end
  end

  return dataPoints
end

function sortData(dataPoints)
  # Copy the keys ourselves into a vector, to prevent having to grow the vector
  # If we would have just called collect, it would have had to grow the vector
  ks = Vector{String}(undef, length(dataPoints))
  i = 1
  for k in keys(dataPoints)
    ks[i] = k
    i += 1
  end
  return sort!(ks, alg=QuickSort)
end

function printData(dataPoints)
  print("{")

  sortedKeys = sortData(dataPoints)

  io = IOBuffer()

  for key in sortedKeys
    value = dataPoints[key]
    print(key)
    print("=")
    print(round(value.min, digits=1))
    print("/")
    print(round(value.sum/value.amountOfPoints, digits=1))
    print("/")
    print(round(value.max, digits=1))
    print(", ")
  end

  print("}")
end

function mergeResults(fullResult::Dict{String, Measurement}, partialResults::Dict{String, Measurement})
  global testLock

  @inbounds for partialMeasurement in partialResults
    lock(testLock)
    existingPoint = get!(fullResult, partialMeasurement.first) do
      # This do part, is to make sure that it only happens when the item does not exist. 
      # If we do not use a do part, it will always run the code for inserting
      partialMeasurement.second
    end
    unlock(testLock)

    if existingPoint != partialMeasurement.second 
      @atomic min(existingPoint.min, partialMeasurement.second.min)
      @atomic max(existingPoint.max, partialMeasurement.second.max)
      @atomic existingPoint.sum += partialMeasurement.second.sum
      @atomic existingPoint.amountOfPoints += partialMeasurement.second.amountOfPoints
    end
  end
end

function solve()
  GC.enable(false) # Disable GC, as we run for a short period of time anyways, and there is no point in dealing with the overhead

  # *10 seems to give the most optimal result when ran with 15 threads. 
  # It might have to do with that reading speed of the file is the limiting factor, so if you make the chunkSize too large, most threads spend their time doing waiting for data
  amountOfThreads = nthreads() * 10
  chunkSize = fld(amountOfLines, amountOfThreads)

  mergedResults = Dict{String, Measurement}()
  sizehint!(mergedResults, 10_000) # The challange said that there could at max be 10_000 stations

  open("./data/measurements_100m.txt") do io
    i = 1
    lines = Vector{String}(undef, amountOfLines)
    @sync for line in eachline(io)
      lines[i] = line
      i += 1

      if i % chunkSize == 0
        t = fld(i, chunkSize)
        lo = i - chunkSize + 1
        hi = t == amountOfThreads ? amountOfLines : t * chunkSize

        @spawn begin 
          dataView = @view lines[lo:hi]
          dataPoints = processData(dataView)
          mergeResults(mergedResults, dataPoints)
        end
      end
    end
  end

  printData(mergedResults)
end

end
