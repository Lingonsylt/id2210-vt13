package search.system.peer.search;

import common.peer.PeerAddress;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import se.sics.kompics.web.WebResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class WebService {
    private static final Logger logger = LoggerFactory.getLogger(WebService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Timer> timerPort;
    Search.TriggerDependency triggerDependency;
    Negative<Web> webPort;
    IndexAddService indexAddService;
    IndexingService indexingService;

    public WebService(Search.TriggerDependency triggerDependency, IndexAddService indexAddService, IndexingService indexingService, PeerAddress self, Negative<Web> webPort, Positive<Timer> timerPort) {
        this.triggerDependency = triggerDependency;
        this.indexAddService = indexAddService;
        this.indexingService = indexingService;
        this.self = self;
        this.timerPort = timerPort;
        this.webPort = webPort;
    }

    public Handler<WebRequest> handleWebRequest = new Handler<WebRequest>() {
        public void handle(WebRequest event) {
            if (event == null || self.getPeerAddress() == null) {
                System.out.println("######  BONKERS!!!!!!!!!!!!!!!!!!!!  ########");
                return;
            }
            if (event.getDestination() != self.getPeerAddress().getId()) {
                return;
            }

            final String SEARCH_COMMAND = "search", ADD_COMMAND = "add", INSPECT_OVERLAY_COMMAND = "inspect";
            List<String> allowedCommands = Arrays.asList(SEARCH_COMMAND, ADD_COMMAND, INSPECT_OVERLAY_COMMAND);

            logger.debug("Handling Webpage Request");

            org.mortbay.jetty.Request jettyRequest = event.getRequest();
            String pathInfoString = jettyRequest.getPathInfo();
            String command = "";
            if (pathInfoString != null && !pathInfoString.equals("")) {
                String[] pathInfos = event.getRequest().getPathInfo().split("/");
                if (pathInfos.length != 0) {
                    command = pathInfos[pathInfos.length - 1];
                }
            }
            WebResponse response;
            if (!allowedCommands.contains(command)) {
                response = WebHelpers.createBadRequestResponse(event, "Invalid command!: " + command);
            } else {
                if (command.equals(INSPECT_OVERLAY_COMMAND)) {
                    response = WebHelpers.createDefaultRenderedResponse(event, "Overlay drawn!", "By node " + self.getPeerId());
                    ScheduleTimeout rst = new ScheduleTimeout(1);
                    rst.setTimeoutEvent(new InspectTrigger(rst));
                    triggerDependency.trigger(rst, timerPort);

                } else if (command.equals(SEARCH_COMMAND)) {
                    String queryString = WebHelpers.getParamOrDefault(jettyRequest, "query", null);
                    if (queryString != null) {
                        String queryResult = null;
                        try {
                            queryResult = indexingService.query(queryString);
                        } catch (IOException e) {
                            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
                        } catch (ParseException e) {
                            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
                            e.printStackTrace();
                        }
                        if (queryResult != null) {
                            response = WebHelpers.createDefaultRenderedResponse(event, "Search succeded!", queryResult);
                        } else {
                            response = WebHelpers.createErrorResponse(event, "Failure searching for " + queryString + "!<br />");
                        }
                    } else {
                        response = WebHelpers.createBadRequestResponse(event, "Invalid query value");
                    }

                } else if (command.equals(ADD_COMMAND)) {
                    String key = WebHelpers.getParamOrDefault(jettyRequest, "key", null);
                    String value = WebHelpers.getParamOrDefault(jettyRequest, "value", null);
                    if (key != null && value != null) {
                        IOException addException = null;
                        indexAddService.addEntryAtClient(key, value);
                        if (addException == null) {
                            response = WebHelpers.createDefaultRenderedResponse(event, "Uploaded item into network!", "Added " + key + " with value " + value + "!");
                        } else {
                            response = WebHelpers.createErrorResponse(event, "Failure adding " + key + " with value " + value + "!<br />" + addException.getMessage());
                        }
                    } else {
                        response = WebHelpers.createBadRequestResponse(event, "Invalid key or value");
                    }
                } else {
                    response = WebHelpers.createBadRequestResponse(event, "Invalid command");
                }
            }
            System.out.println("Ending web request");
            triggerDependency.trigger(response, webPort);
        }
    };

    /**
     * TODO: Remove machine specific paths in Snapshot.inspectOverlay
     * Draw the overlay using graphviz neato and display it using eye of gnome (requires graphviz and eye of gnome)
     */
    Handler<InspectTrigger> handleInspectTrigger = new Handler<InspectTrigger>() {
        public void handle(InspectTrigger trigger) {
            tman.simulator.snapshot.Snapshot.inspectOverlay(self.getPeerId());
        }
    };

    static class WebHelpers {
        public static WebResponse createBadRequestResponse(WebRequest event, String message) {
            return new WebResponse(createBadRequestHtml(message), event, 1, 1);
        }

        public static String getParamOrDefault(org.mortbay.jetty.Request jettyRequest, String param, String defaultValue) {
            return (jettyRequest.getParameter(param) == null ||
                    jettyRequest.getParameter(param).equals("")) ?
                    defaultValue : jettyRequest.getParameter(param);
        }

        public static WebResponse createErrorResponse(WebRequest event, String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", "Error!");
            params.put("message", message);
            return new WebResponse(renderHtmlTemplate(getDefaultHtmlTemplate(), params), event, 1, 1);
        }

        public static WebResponse createDefaultRenderedResponse(WebRequest event, String title, String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", title);
            params.put("message", message);
            return new WebResponse(renderHtmlTemplate(getDefaultHtmlTemplate(), params), event, 1, 1);
        }

        public static String getDefaultHtmlTemplate() {
            StringBuilder sb = new StringBuilder("<!DOCTYPE html PUBLIC \"-//W3C");
            sb.append("//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR");
            sb.append("/xhtml1/DTD/xhtml1-transitional.dtd\"><html xmlns=\"http:");
            sb.append("//www.w3.org/1999/xhtml\"><head><meta http-equiv=\"Conten");
            sb.append("t-Type\" content=\"text/html; charset=utf-8\" />");
            sb.append("<title>Adding an Entry</title>");
            sb.append("<style type=\"text/css\"><!--.style2 {font-family: ");
            sb.append("Arial, Helvetica, sans-serif; color: #0099FF;}--></style>");
            sb.append("</head><body><h2 align=\"center\" class=\"style2\">");
            sb.append("ID2210 {{ title }}</h2><br>{{ message }}</body></html>");
            return sb.toString();
        }

        public static String renderHtmlTemplate(String html, Map<String, String> params) {
            for (String key : params.keySet()) {
                html = html.replaceAll("\\{\\{[\\s]?" + key + "[\\s]?}}", params.get(key));
            }
            return html;
        }

        public static String createBadRequestHtml(String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", "Bad request!");
            params.put("message", message);
            return renderHtmlTemplate(getDefaultHtmlTemplate(), params);
        }
    }
}
