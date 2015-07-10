module WorkerRoulette
  module Preprocessor
    def preprocess(work_order, channel)
      return work_order unless preprocessors.any?

      class_name = self.class.name.split(/::/).last

      preprocessors.inject(work_order) do |job, processor_module|
        processor_class = processor_module.const_get(class_name)
        processor = processor_class.new
        processor.process(job, channel)
      end
    end
  end
end
