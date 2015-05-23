package com.khovanskiy.dkvstorage.vr.message;

import com.khovanskiy.dkvstorage.vr.operation.DeleteOperation;
import com.khovanskiy.dkvstorage.vr.operation.GetOperation;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.operation.SetOperation;

import java.text.ParseException;

/**
 * @author Victor Khovanskiy
 *
 * @deprecated
 */
public class MessageHandler {
    private String[] slices;
    private int p;

    public MessageHandler(String string) {
        this.slices = string.split(" ");
    }

    public Message parse() throws ParseException {
        return parseMessage();
    }

    private Message parseMessage() throws ParseException {
        switch (curToken()) {
            case "node": {
                nextToken();
                return parseIdentificationMessage();
            }
            case "prepare": {
                nextToken();
                //return parsePrepareMessage();
            }
            case "prepareok": {
                nextToken();
                return parsePrepareOkMessage();
            }
            case "VALUE":
            case "PONG":
            case "STORED":
            case "NOT_FOUND":
            case "ACCEPTED": {
                //return parseReplyMessage();
            }
            default: {
                return parseRequestMessage();
            }
        }
    }

    /*private Message parseReplyMessage() throws ParseException {
        return new ReplyMessage(curToken());
    }*/

    private Message parsePrepareOkMessage() {
        return null;
    }

    /*private PrepareMessage parsePrepareMessage() throws ParseException {
        int viewNumber = nextInt();
        RequestMessage request = parseRequestMessage();
        int opNumber = nextInt();
        int commitNumber = nextInt();
        return new PrepareMessage(viewNumber, request, opNumber, commitNumber);
    }*/

    private Message parseIdentificationMessage() throws ParseException {
        int id = nextInt();
        return new IdentificationMessage(id);
    }

    private RequestMessage parseRequestMessage() throws ParseException {
        Operation operation = parseOperation();
        int clientId = nextInt();
        int requestNumber = nextInt();
        return new RequestMessage(operation, clientId, requestNumber);
    }

    private int nextInt() throws ParseException {
        try {
            int res = Integer.parseInt(curToken());
            nextToken();
            return res;
        } catch (NumberFormatException exception) {
            throw new ParseException(exception.getMessage(), p);
        }
    }

    private String curToken() throws ParseException {
        if (p >= slices.length) {
            throw new ParseException("Too litte parts", p);
        }
        return slices[p];
    }

    private String nextToken() throws ParseException {
        String res = curToken();
        ++p;
        return res;
    }

    private boolean nextBoolean() throws ParseException {
        boolean res = Boolean.parseBoolean(curToken());
        nextToken();
        return res;
    }

    public Operation parseOperation() throws ParseException {
        switch (curToken()) {
            case "get": {
                nextToken();
                String key = nextToken();
                return new GetOperation(key);
            }
            case "set": {
                nextToken();
                String key = nextToken();
                String value = nextToken();
                return new SetOperation(key, value);
            }
            case "delete": {
                nextToken();
                String key = nextToken();
                return new DeleteOperation(key);
            }
        }
        throw new ParseException("unknown operation: " + curToken(), p);
    }
}
