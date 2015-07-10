require "spec_helper"

describe WorkerRoulette::Preprocessor do
  before { allow(subject).to receive(:preprocessors).and_return(preprocessors) }

  class TestClass
    include WorkerRoulette::Preprocessor
    def preprocessors
    end
  end

  module TestPreprocessor
    class TestClass
      def process(job, channel)
      end
    end
  end

  describe "#preprocess" do
    let(:wo) { double("work_order") }
    let(:result) { double("resulting_wo") }
    let(:channel) { "aChannel" }
    subject { TestClass.new }

    context "with one preprocessor" do
      let(:preprocessors) { [TestPreprocessor] }

      it "calls the correct preprocessor with the correct args" do
        expect_any_instance_of(TestPreprocessor::TestClass).to receive(:process).with(wo, channel)
        subject.preprocess(wo, channel)
      end

      it "returns the value of the preprocessor" do
        allow_any_instance_of(TestPreprocessor::TestClass).to receive(:process).and_return(result)
        expect(subject.preprocess(wo, channel)).to eq(result)
      end
    end

    context "with two preprocessors" do
      let(:preprocessors) { [TestPreprocessor, TestPreprocessor] }
      let(:intermediate) { double("intermediate_result") }

      it "chains the preprocessors and returns the correct result" do
        allow_any_instance_of(TestPreprocessor::TestClass).to receive(:process).with(wo, channel).and_return(intermediate)
        allow_any_instance_of(TestPreprocessor::TestClass).to receive(:process).with(intermediate, channel).and_return(result)

        expect(subject.preprocess(wo, channel)).to eq(result)
      end
    end
  end
end
