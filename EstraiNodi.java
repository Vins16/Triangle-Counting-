package gebd.spark;

import org.apache.spark.api.java.function.Function;

/*
 * Classe utilizzata per estrarre da ciascuna riga del file txt
 * gli archi, formati da 'nodo FROM' e 'nodo TO' presenti nel suo interno incapsulandoli 
 * in una istanza della classe Edge
 */

public class EstraiNodi implements Function<String, Edge> {

	@Override
	public Edge call(String line) throws Exception {
		
		String[] values = line.split("\t");
		Edge is = new Edge();
	
		String from= values[0];
		is.setFrom(from);
		
		
		String to  = values[1];
		is.setTo(to);
		
		return is;
	}

	

}
