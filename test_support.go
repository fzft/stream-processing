package stream_processing

// TestMode describe current test mode
type TestMode struct {
	doSnapshots bool
	restoreInterval int
	inboxLimit int
}

func NewTestMode(doSnapshots bool, restoreInterval int, inboxLimit int) *TestMode {
	return &TestMode{doSnapshots: doSnapshots, restoreInterval: restoreInterval, inboxLimit: inboxLimit}
}


// TestProcessor a utility to test processors. it will initialize the processor instance, pass input items to it and assert outbox context
type TestProcessor struct {
	metaSupplier ProcessorMetaSupplier
	supplier ProcessorSupplier
	inputs [][]interface{}
	priorities []int
	assertProgress bool
	callComplete bool

	localProcessorIndex int
	localParallelism int
	totalParallelism int
	outputOrdinalCount int

	assertOutputFn BiAcceptFn
}


func NewTestProcessorWithSupplier(supplier GetFn) *TestProcessor {
	p := new(TestProcessor)
	p.metaSupplier = NewMetaSupplierFromProcessorSupplier(LOCAL_PARALLELISM_USE_DEFAULT, supplier)
	return p
}

// input Sets the input objects for processor the input can contain watermark, deliver to the Processor
func (p *TestProcessor) input(input []interface{}) *TestProcessor {
	p.inputs = [][]interface{}{input}
	p.priorities = []int{0}
	return p
}

func (p *TestProcessor) expectOutput(expectedOutputs [][]interface{}) {
	p.assertOutput(len(expectedOutputs), func(mode, actual interface{}) {
		p.assertExpectedOutput(mode, expectedOutputs, actual)
	})
}

// assertOutput run the test with the specified custom assertion
func (p *TestProcessor) assertOutput(outputOrdinalCount int, f BiAcceptFn) {
	p.outputOrdinalCount = outputOrdinalCount
	p.assertOutputFn = f
	p.runTest(NewTestMode(false, 0, 1))
}

func (p *TestProcessor) runTest(mode *TestMode) {
	
}

func (p *TestProcessor) assertExpectedOutput(mode interface{}, outputs [][]interface{}, actual interface{}) {
	
}




