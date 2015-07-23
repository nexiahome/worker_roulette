require "spec_helper"

module QueueMetricTracker
  describe StatCalculator do
    let(:granularity)         { 10 }
    let(:default_granularity) { 100 }
    subject(:calculator) { described_class }

    it "responds to new with a granularity" do
      expect(calculator.new(granularity).granularity).to eq(granularity)
    end

    it "granularity defaults to a value" do
      expect(calculator.new().granularity).to eq(default_granularity)
    end

    describe "#add" do
      let(:granularity)         { 3 }
      let(:value1)  { 4 }
      let(:value2)  { 3 }
      let(:value3)  { 8 }
      let(:average) { 5 }
      subject(:calculator) { described_class.new(granularity) }

      it "calculates average of N values" do
        expect(subject.add(value1)).to be_nil
        expect(subject.add(value2)).to be_nil
        expect(subject.add(value3)).to eq(average)
      end
    end
  end
end
