class String
  def underscore
    self.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
  end
end
class Hash
  #  FROM: File activesupport/lib/active_support/core_ext/hash/keys.rb, line 50
  #  made recursive by JB
  def symbolize_keys!
    transform_keys!{ |key| key.to_sym rescue key }
  end

  def transform_keys!
    return enum_for(:transform_keys!) unless block_given?
    keys.each do |key|
      value = delete(key)
      self[yield(key)] = value.kind_of?(Hash) ? value.symbolize_keys! : value
    end
    self
  end
end
