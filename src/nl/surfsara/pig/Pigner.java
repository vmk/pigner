// Copyright (c) 2013 SURFsara
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
 package nl.surfsara.pig;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

/**
 * <p> Pigner uses the Stanford Named Entity Recognizer: <a href="http://nlp.stanford.edu/ner/">http://nlp.stanford.edu/ner/</a>
 * to perform a basic classification (NER) on input text lines. </p>
 * 
 * <p> Pigner is an implementation of a Pig user defined function for use in pig latin scripts.
 * See the demo script for pig latin details (pignerdemo.pig). </p> 
 * 
 * <p> Pigner is provided as a proof of concept or demo to illustrate how third party java code
 * can be used in pig. </p> 
 * 
 * @author vm.kattenberg - mathijs.kattenberg@surfsara.nl
 */
public class Pigner extends EvalFunc<DataBag> {
	private static BagFactory bf = BagFactory.getInstance();
	private static TupleFactory tf = TupleFactory.getInstance();
	PigStatusReporter reporter = PigStatusReporter.getInstance();

	private static enum counters {
		WRONG_INPUT
	};

	/*
	 * Performs classification on each input tuple. Input tuples should consist of a single String of text.
	 * The function outputs (tag,term) tuples for each term that represents an entity (person,organization, etc.).
	 */
	@SuppressWarnings({ "unchecked"})
	@Override
	public DataBag exec(Tuple input) throws IOException {
		// Check for null input
		if (input == null) {
			if (reporter != null) {
				reporter.getCounter(counters.WRONG_INPUT).increment(1);
			}
			return null;
		}
		DataBag returnBag = bf.newDefaultBag();
		try {
			String text = ((DataByteArray) input.get(0)).toString();
			
			// Load classifier from resource; For proof of concept only one classifier is provided from the Stanford NER package
			AbstractSequenceClassifier<CoreLabel> classifier = ((AbstractSequenceClassifier<CoreLabel>) CRFClassifier.getClassifier(Pigner.class.getResourceAsStream("/nl/surfsara/pig/resources/english.all.3class.distsim.crf.ser")));
			
			// Classify text supplied to UDF		
			 List<List<CoreLabel>> classify = classifier.classify(text);
		        for (List<CoreLabel> coreLabels : classify) {
		            for (CoreLabel coreLabel : coreLabels) {
		                String word = coreLabel.word();
		                String answer = coreLabel.get(CoreAnnotations.AnswerAnnotation.class);
		                if(!"O".equals(answer)){
		                	// Add (tag,term) tuples to output; outputting only entitities (not "O")
		                	Tuple t = tf.newTuple();
							t.append(answer);
							t.append(word);
							returnBag.add(t);
		                }
		            }
		        }
		} catch (ClassCastException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return returnBag;
	}
}
