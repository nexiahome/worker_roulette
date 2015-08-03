module QueueMetricTracker
  class StatCalculator
    attr_accessor :count, :sum, :granularity

    def initialize(granularity = 100)
      @granularity = granularity
      @sum = 0
      @count = 0
    end

    def add(value)
      @sum   += value
      @count += 1

      if @count == granularity
        value = @sum / granularity
        @sum = @count = 0
        return value
      end

      return nil
    end
  end
end
