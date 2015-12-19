package com.acloudfan.mqsender;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import com.ibm.mqlight.api.CompletionListener;
import com.ibm.mqlight.api.NonBlockingClient;
import com.ibm.mqlight.api.NonBlockingClientAdapter;
import com.ibm.mqlight.api.QOS;
import com.ibm.mqlight.api.SendOptions;

@Path("/quote")
public class MQSender {

	private static final String PUBLISH_TOPIC = "javamqlight/quotes";
	
	/** Simple logging */
	private final static Logger logger = Logger.getLogger(MQSender.class.getName());
	
	/** Declare the non blocking MQ client **/
	private NonBlockingClient mqlightClient;
	
	// Constructor
	public MQSender(){
		NonBlockingClientAdapter adaptor = createNonBlockingClientAdaptor();
		mqlightClient = NonBlockingClient.create(null, adaptor, null);
	}
	
	@GET
	public String sendMessage(@QueryParam("msg") String msg){
		// options
		SendOptions opts = SendOptions.builder().setQos(QOS.AT_LEAST_ONCE).setTtl(600000).build();
		
		// now we will send
		mqlightClient.send(PUBLISH_TOPIC, msg, null, opts, new CompletionListener<Void>() {
            public void onSuccess(NonBlockingClient client, Void context) {
              logger.log(Level.INFO, "Client id: " + client.getId() + " sent message!");
            }
            public void onError(NonBlockingClient client, Void context, Exception exception) {
              logger.log(Level.INFO,"Error!." + exception.toString());
            }
        }, null);
		
		return "SUCCESS Msg Send = "+ msg;
	}
	

	
	/** Listener for events generated on client **/
	private NonBlockingClientAdapter<Void> createNonBlockingClientAdaptor(){
		return new NonBlockingClientAdapter<Void>(){

			@Override
			public void onStarted(NonBlockingClient client, Void context) {
				
				super.onStarted(client, context);
				logger.info("SENDER - Client Started");
			}
			
		};
	};
	
	
}
