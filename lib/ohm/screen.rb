module Ohm

  # Screen provides a simple identity map layer so that a thread running a transaction
  # with Ohm objects always refers to the same object once loaded from the data
  # store. This also saves multiple loads of the same objects.
  #
  # The application client can clear the Screen at any time, typically at the
  # end of a transaction and/or request cycle for web applications. Servers with
  # process affinity or sticky session type mechanisms may want to keep the warm
  # Screen across requests.
  #
  module Screen

    class Screen < Hash
      
      def counts
        @counts ||= {}
      end
      
      def clear
        @counts = nil
        super
      end
  
      def delete(mk)
        mk.respond_to?(:key) ? super(mk.key) : super(mk.to_s)
      end
  
      def [](k)
        hit, miss = counts[k] || [0,0]
        if ( r = super )
          hit += 1
        else
          miss += 1
        end
       counts[k] = [hit, miss]
       r
      end
    end

    def self.included(base)
      base.extend ClassMethods
    end

    module ClassMethods

      # The current Ohm::Screen in use by this thread
      # This could also be specialized per class, e.g. Ohm::Screen.current[root]
      def screen
        Ohm.threaded[:screen] ||= Screen.new
      end
    end

  end
end

