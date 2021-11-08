package gebd.spark;

import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import scala.Tuple2;


public class CliqueCounting {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
	
		/*
		 * inizializzazione dell'ambiente da realizzare
		 * attraverso l'oggetto SparkConf
		 */
		
		SparkConf sc = new SparkConf();
		
		sc.setAppName("Clique Counting");
		
		sc.setMaster("local[*]");
		
		
		/*
		 * instanziamo il framework spark
		 */
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		/*
		 * carico il file txt 
		 */
		

		JavaRDD<String> dEdges = jsc.textFile("data/"+ "p2p-Gnutella08"+".txt"); //3 s
		//JavaRDD<String> dEdges = jsc.textFile("data/"+ "p2p-Gnutella31"+".txt"); //12 s
		
		//JavaRDD<String> dEdges = jsc.textFile("data/"+ "Amazon0302"+".txt");// meno di 2 min
		//JavaRDD<String> dEdges = jsc.textFile("data/"+ "Amazon0312"+".txt"); //22 min

		
		/*
		 * elimino le righe  dell'intestazione che iniziano con '#'
		 */
		dEdges = dEdges.filter(l -> !l.startsWith("#"));
		
		//System.out.println(dEdges.take(5));
		
		/*
		 * Obiettivo di questa classe è di contare i triangoli formati dai vari archi. 
		 * Per creare un triangolo, ho bisogno di 3 archi e 3 nodi
		 * AB->BC->CA
		 * 0. Creo una JavaPairRDD che avrà come chiave i nodi che lo formano ciascun arco e  come valore  "0"
		 * 1. Ordino gli archi estratti secondo il principio total order
		 * 2. Per ogni nodo trovo l'insieme dei nodi adiacenti
		 * 3. Conservo solo i nodi che hanno almeno 2 vicini
		 * 4. Per ogni sottoinsieme dei nodi adiacenti estraggo le coppie e tengo traccia del nodo "responsabile"
		 * 5. Per ogni coppia, trovo l'insieme di tutti i nodi responsabili
		 * 6. Per ogni coppia, verifico che esista un arco tra i due nodi
		 * 7. Estraggo tutti i nodi responsabili di tutti i triangoli
		 * 8. Per ogni nodo responsabile, conto il numero di triangoli di cui è responsabile
		 */
	
		
		//MAP1
		
		/*
		 * per ogni arco estraggo i nodi utilizzando il metodo EstraiNodi
		 */
		
		JavaRDD<Edge> dEdges1 = dEdges.map(new EstraiNodi());
		
		/*
		 * Creo una JavaPairRDD che ha come chiave l'arco(x;y) e come valore "0"
		 */
		
		JavaPairRDD<String, String> dAB= dEdges1.mapToPair( is-> new Tuple2<String, String>(is.getFrom()+";"+is.getTo(),"0"));

		/*
		 * Ordino gli archi secondo il principio total order
		 * Ciascun arco viene considerato solo una volta
		 */
		
		// estraggo le chiavi, che rappresentano gli archi (x;y)
		JavaRDD<String> dEdges2 = dAB.keys();
		//System.out.println(dEdges2.take(12));
		
		JavaPairRDD<String, String> dEdgesOrdered2 = dEdges2.flatMapToPair(new ExtractOrderedEdges2()).distinct();
		System.out.println();
		System.out.println("Coppie di nodi (x,y) ordinate");
		System.out.println(dEdgesOrdered2.take(15)+"\n");
	
		//REDUCE 1
		
		/*
		 * per ogni nodo cerco i suoi nodi adiacenti attraverso 'reduceByKey'
		 */
		JavaPairRDD<String, String> dNeighbors = dEdgesOrdered2.reduceByKey((v,u)->v+";"+u);
		//System.out.println("Coppia <Nodo, Vicini>)");
		//System.out.println(dNeighbors.take(5)+"\n");
	
		/*
		 * Scarto i nodi che hanno un solo vicino perchè sicuro non contribuiscono a creare un triangolo
		 */
		JavaPairRDD<String,String> dNeighbors3 = dNeighbors.filter(tupla-> tupla._2.split(";").length>=2);
		System.out.println("Nodi \"responsabili\" con almeno due vicini");
		System.out.println(dNeighbors3.take(15)+"\n");
		
		
		// MAP 2
		
		// Creo una JavaPairRDD <x;y , $>  
		JavaPairRDD<String, String> dEdgesOrdered = dEdges2.flatMapToPair(new ExtractOrderedEdges()).distinct();
		System.out.println("Coppie di nodi (x;y) ordinate,$");
		System.out.println(dEdgesOrdered.take(15)+"\n");

		/*
		 * per ogni nodo responsabile mi ricavo le coppie dei nodi adiacenti e tengo traccia
		 * del nodo 'responsabile'.
		 * (x;y, u)
		 */
		
		JavaPairRDD<String,String> dCoppiaVicini = dNeighbors3.flatMapToPair( new ExtractPairs2());
		System.out.println("Coppie <x;y , u>");
		System.out.println(dCoppiaVicini.take(15)+"\n");
		

		
		
		
		// REDUCE 2
		
		JavaPairRDD<String,String> dCoppiaVicini2 = dCoppiaVicini.reduceByKey((v,u)->v+";"+u);
		//System.out.println("Coppie <A;B , Nodi Responsabili>");
		//System.out.println(dCoppiaVicini2.take(5)+"\n");
		
		/*
		 * si considera triangolo il subgrafo composto da 3 nodi e 3 archi
		 * Perciò per ogni coppia A;B verifico che esista un arco tra loro.
		 * Per questo si esegue un join tra (A;B , Nodi Responsabili) e  le coppie (A;B,$) ordinate,
		 * sfruttando il fatto che anche nel caso (A;B , Nodoresponsabile) le coppie (A;B)sono ordinate
		 */

		/*
		 * il JOIN restituisce una Nuova JavaPairRDD che contiene solo le coppie 
		 * di cui la chiave è presente nelle due RDD
		 */
		JavaPairRDD<String, Tuple2<String, String>> dTriangoli2 = dCoppiaVicini2.join(dEdgesOrdered);
		System.out.println("<A;B con arco, Nodi Responsabili>:");
		System.out.println(dTriangoli2.take(10)+"\n");
		
        // MAP 3

		/*
		 * processiamo tutte le tuple contenti <A;B , (u1,...uk)>
		 * Ciascuna nodo responsabile andra ad alimentare, sotto forma di chiave,
		 * una nuova JavaPairRDD a cui assoceremo come valore l'arco A;B -> <ui, A;B>
		 */
		
	
		JavaPairRDD<String, String> dNodiResponsabili2 =dTriangoli2.flatMapToPair(new EstraiNodiResponsabili2());
		System.out.println("<NodoResponsabile,A;B>");
		System.out.println(dNodiResponsabili2.take(10)+"\n");
		
		//REDUCE 3
		
		JavaRDD<String>dNodiResponsabili3 =dNodiResponsabili2.keys();
		
		/*
		 * processiamo le stringhe contenti i nodi responsabili di ciascun triangolo
		 * Ciascun nodo responsabile andra ad alimentare, sotto forma di chiave,
		 * una nuova JavaPairRDD a cui assoceremo il valore 1
		 * Infine si raggruppano tutte le coppie (nodoResponsabile, 1) che hanno come chiave
		 * lo stesso nodoResponsabile e si sommano i valori 1,
		 * ottenendo cosi una nuova JavaPairRDD<String, Integer> che avra
		 * come chiave il nodoResponsabile e come valore
		 *  il numero di triangoli di cui è responsabile
		 */
		JavaPairRDD<String, Integer> dNodiResponsabili4= dNodiResponsabili3.flatMapToPair(new EstraiNodiResponsabili());
		dNodiResponsabili4 =dNodiResponsabili4.reduceByKey((u,v)->u+v);
		System.out.println("<NodoResponsabile,n° triangoli>");
		System.out.println(dNodiResponsabili4.take(10)+"\n");
		
		//VERIFICA:
		
		/*
		 * Calcolo il totale dei triangoli per verificare che siano
		 * 2383 come indico sul sito SNAPA
		 */
		
		 JavaRDD<Integer>dNumTriangoli =dNodiResponsabili4.values();
			
		 Integer total = dNumTriangoli.reduce((v1, v2) -> v1+v2);
		 System.out.println("Total: "+total);
		
		// N e o 4 j
		 
		/*
		 * stabilisco una connessione con il db neo4j 
		 */
/*
        String uri= "bolt://localhost:7687";
		AuthToken token = AuthTokens.basic("neo4j", "1234");
		Driver driver = GraphDatabase.driver(uri, token);
		Session s = driver.session();
		
		/*
		 * carico il contenuto di contaTriangoli sul grafo di Neo4j:
		 * 1. carico tutti i nodi solo una volta, indipentemente se siano
		 *    "FROM" o "TO"
		 */

		/*
		 * scarico nella memoria il contenuto di dEdgesOrdered2,
		 * ovvero gli archi ordinati
		 */
		
//		List<Tuple2<String, String>>archi = dEdgesOrdered2.collect();
		
		// creo una lista che va a ospitare tutti i nodi dell'arco
/*		List<String> nodi = new ArrayList<String>();
		
		for(Tuple2<String, String> arco: archi) {
		
			String from= arco._1;
			String to = arco._2;
			
*/
			/*
			 * Dapprima verifico se un nodo è già stato caricato
			 * utilizzando l'ArrayList nodi.
			 * Se il nodo non è stato caricato, lo carico
			 */
/*
			if(!nodi.contains(from)) {
				nodi.add(from);
				String cql = "Create(n1:Nodo {id:"+from+"})";
				s.run(cql);
			}
			if(!nodi.contains(to)) {
				nodi.add(to);
			   
				String cql = "Create(n2:Nodo{id:"+to+"})";
			    s.run(cql);	
			}

		}
		
*/		
		/*
		 * 2. Carico gli archi ordinati 
		 */
/*		
		for(Tuple2<String, String> arco: archi) {
			String from= arco._1;
		
			String to = arco._2;
		    
		    String cql = "match(n1:Nodo {id:"+from+"}), (n2:Nodo{id:"+to+"})"
		               + "create (n1)-[:arco]->(n2)";

		               
		    s.run(cql);
		}
*/		
		/*
		 * ad ogni nodo responsabile aggiungo la proprietà 
		 * nodoResponsabile=True
		 */
		
		/*
		 * scarico in memoria i nodi responsabili contenuti 
		 * in dNodiResponsabili2 come chiave
		 */
		
/*	
		List<String> nodiResponsabili = dNodiResponsabili2.keys().collect(); 
		for(String nodoResponsabile: nodiResponsabili) {
			String cql ="match (n1:Nodo{id:"+nodoResponsabile+"}) set n1.nodoResponsabile=True";             
		    s.run(cql);
		}
*/	

		/*
		 * Cerco quali sono e quanti sono i nodi ritenuti "responsabili"
		 */
/*	
		String cql = "match(n:Nodo{nodoResponsabile:True}) return count(n)";
        Result result = s.run(cql); 
        Record r1 = result.next();
        System.out.println("Numero totale di nodi considerati \"responsabili\" ");
	    System.out.println(r1.get("count(n)")+"\n");
		
		
		cql = "match(n:Nodo{nodoResponsabile:True}) return n.id limit 25";
        result = s.run(cql);
		System.out.println("25 Nodi su "+r1.get("count(n)")+" ritenuti responsabili:");
 		while(result.hasNext()) {

 			Record r2 = result.next();
 			System.out.print(r2.get("n.id")+"; ");
 		}
 		System.out.print("\n"+"\n");
 		
		
		/*
		 * Ad ogni nodo per ogni triangolo a cui appartiene assegno 
		 * una nuova label triangolo(i), dove i è il codice identificativo
		 * del triangolo
		 */
		 
/*		 //creo una variabile int i che mi serve per numerare i triangoli
		 int i=1;
		 
		 List<Tuple2<String, String>>triangoli = dNodiResponsabili2.collect();
		 for(Tuple2<String, String> triangolo: triangoli) {
			 String nodoResponsabile= triangolo._1;
			 String[] nodiAdiacenti = triangolo._2.split(";");
			 String nodoA =nodiAdiacenti [0];
			 String nodoB =nodiAdiacenti [1];
			 
			 String cql = "MATCH p = (n1:Nodo {id:"+nodoResponsabile+"})-[]-(n2:Nodo {id:"+nodoA+"})-[]-(n3:Nodo {id:"+nodoB+"})"
					 +"FOREACH (n IN nodes(p) | set n:triangolo"+i+")" ;
			 s.run(cql);
			 i++;
		 }
*/		 
		 
		 /*
		  * Cerco  quanti e quali sono i nodi che non hanno partecipato
		  * nemmeno a un triangolo e ottengo il loro ID
		  */
       // String cql = "match(n) where size(labels(n))<2 return count(n)";
       // Result result = s.run(cql); 
/*        Record r3 = result.next();
        System.out.println("Numero totale di nodi che non partecipano a nessun triangolo");
	    System.out.println(r3.get("count(n)")+"\n");
		
        cql = "match(n) where size(labels(n))<2 return n.id limit 25";
        result = s.run(cql);
 		
        System.out.println("25 Nodi su "+r3.get("count(n)")+" che non partecipano a nessun triangolo:");
 		while(result.hasNext()) {

 			Record r4 = result.next();
 			System.out.print(r4.get("n.id")+"; ");
 		}
 		System.out.print("\n"+"\n");
 		
		/*
		 * Cerco  quanti e quali sono i nodi che hanno partecipato
		 * a più di 100 triangoli
		 */
/*		
 		cql = "match(n) where size(labels(n))>100 return count(n)";
        result = s.run(cql); 
        Record r5 = result.next();
        System.out.println("Numero totale di nodi che hanno partecipano a più di 100 triangoli");
	    System.out.println(r5.get("count(n)")+"\n");
		
        cql = "match(n) where size(labels(n))>100 return n.id";
        result = s.run(cql);
 		
        System.out.println("Nodi che hanno partecipano a più di 100 triangoli:");
 		while(result.hasNext()) {

 			Record r6 = result.next();
 			System.out.print(r6.get("n.id")+"; ");
 		}
 		
 		System.out.print("\n"+"\n");
 		
 		
 		
 		/*
 		 * Cerco prima il massimo numero di volte in cui un nodo ha partecipato
 		 * a formare dei triangoli e in seguito quali sono i nodi
 		 * che hanno partecipato a formare un triangolo
 		 * un numero di volte pari al massimo
 		 */
 /*		
     	cql = "match(n) with max(size(labels(n))) as massimo match (n) where size(labels(n))= massimo return n.id, (massimo-1)";
        result = s.run(cql); 
        Record r7 = result.next();
        System.out.println("Il numero massimo di triangoli a cui ha partecipato un nodo:");
	    System.out.println(r7.get("(massimo-1)")+"\n");
	
        
	    result = s.run(cql); 
  		
         System.out.println("Nodi che hanno partecipato a un triangolo un numero di volte pari al massimo");
  		 while(result.hasNext()) {

  			Record r8 = result.next();
  			System.out.print(r8.get("n.id")+"; ");
  		 }
  		 
  		System.out.print("\n"+"\n");
  		
  		 /*
  		  * Quali sono i triangoli a cui hanno partecipato
  		  * i nodi che hanno partecipato a formare un triangolo
  		  * un numero di volte pari al massimo
  		  */
/*		 
  		cql = "match(n) with max(size(labels(n))) as massimo match (n) where size(labels(n))= massimo return labels(n) limit 100";
	    result = s.run(cql); 
  		
         System.out.println("Triangoli di cui fanno parte i nodi che hanno partecipato a un triangolo un numero di volte pari al massimo");
  		 while(result.hasNext()) {

  			Record r9 = result.next();
  			System.out.print(r9.get("labels(n)")+"; ");
  		 }

 

		 
		/*
		 * classe scanner utilizzata per mantere il programma
		 * in esecuzione
		 */
       Scanner in = new Scanner(System.in); 
        String a = in.nextLine();

}
}
