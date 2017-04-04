package edu.teco.serverless.model.servicelayer.service;

import edu.teco.serverless.auth.authFacade.AuthFacade;
import edu.teco.serverless.model.exception.lambda.LambdaDuplicatedNameException;
import edu.teco.serverless.auth.authFacade.AuthFacadeImpl;
import edu.teco.serverless.model.exception.lambda.LambdaNotFoundException;
import edu.teco.serverless.model.lambda.AuthKey;
import edu.teco.serverless.model.lambda.ExecuteConfig;
import edu.teco.serverless.model.lambda.Identifier;
import edu.teco.serverless.model.lambda.Lambda;
import edu.teco.serverless.model.servicelayer.lambdaruntime.RuntimeController;
import edu.teco.serverless.model.servicelayer.lambdaruntime.communication.RuntimeConnectException;
import edu.teco.serverless.model.servicelayer.lambdaruntime.communication.TimeExceededException;
import edu.teco.serverless.model.servicelayer.lambdaruntime.images.LanguageNotSupportedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

/**
 * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
 */
@Service
public class LambdaManagerFacadeImpl implements LambdaManagerFacade {
    final static Logger logger = Logger.getLogger(LambdaManagerFacadeImpl.class);


    @Autowired
    private RuntimeController runTime;
    @Autowired
    private AuthFacade authenticatior;


    /**
     * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
     */

    public String addLambda(Lambda lambda) throws RuntimeConnectException, IOException, LanguageNotSupportedException, IllegalAccessException, InstantiationException, ClassNotFoundException, TimeExceededException {

        try {
            runTime = RuntimeController.getInstance();
            logger.info("runtTime is created.");
        } catch (IllegalAccessException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (InstantiationException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (ClassNotFoundException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }


        if (runTime.lambdaExists(lambda.getName())) {
            throw new LambdaDuplicatedNameException();
        }

        String token = null;
        try {
            AuthKey authKey = runTime.buildImage(lambda);

            logger.info(String.format("Get authKey for token : %s", authKey.getAuthKey()));
            token = authenticatior.generateMasterToken(authKey, lambda.getName());
            logger.info(String.format("Get token for authorisation : %s", token));
        } catch (UnsupportedEncodingException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (LanguageNotSupportedException e) {

            logger.error("Error, the given language is not supported.", e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            e.printStackTrace();
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            e.printStackTrace();
        }
        return token;

    }

    /**
     * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
     */

    public String executeLambda(String nameOfLambda, ExecuteConfig executeConfig) throws RuntimeConnectException, TimeExceededException, IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        try {
            runTime = RuntimeController.getInstance();
            logger.info("runtTime is created.");
        } catch (IllegalAccessException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (InstantiationException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (ClassNotFoundException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        }
        Identifier identifier = new Identifier(nameOfLambda);
        if (!runTime.lambdaExists(identifier)) {

            throw new LambdaNotFoundException();
        }
        String result = null;
        try {
            result = runTime.run(identifier, executeConfig);
            logger.info(String.format("Get result of execution : %s", result));
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }
        return result;

    }

    /**
     * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
     */
    public String updateLambda(String nameOfLambda, Lambda lambda) throws RuntimeConnectException, LanguageNotSupportedException, TimeExceededException, IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {

        try {
            runTime = RuntimeController.getInstance();
            logger.info("runtTime is created.");
        } catch (IllegalAccessException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (InstantiationException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (ClassNotFoundException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }


        if (!runTime.lambdaExists(new Identifier(nameOfLambda))) {

            throw new LambdaNotFoundException();
        }
        String token = null;
        try {
            AuthKey authKey = runTime.rebuildImage(lambda);
            logger.info(String.format("Get authKey for token : %s", authKey.getAuthKey()));

            token = authenticatior.generateMasterToken(authKey, lambda.getName());
            logger.info(String.format("Get token for authorisation : %s", token));
        } catch (UnsupportedEncodingException e) {
            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (LanguageNotSupportedException e) {

            logger.error("Error, the given language is not supported.", e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }
        return token;
    }

    /**
     * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
     */
    public Lambda getLambda(String nameOfLambda) throws RuntimeConnectException, IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, TimeExceededException {
        try {
            runTime = RuntimeController.getInstance();
            logger.info("runtTime is created.");
        } catch (IllegalAccessException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (InstantiationException e) {
            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (ClassNotFoundException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }

        Lambda lambda;
        Identifier identifier = new Identifier(nameOfLambda);
        if (!runTime.lambdaExists(identifier)) {


            throw new LambdaNotFoundException();
        }
        try {
            lambda = runTime.getLambda(identifier);
            logger.info(String.format("Get lambda : %s", lambda.toString()));
        } catch (FileNotFoundException e) {

            logger.error("Error, this Lambda-function could not be found.", e);
            throw e;
        }

        return lambda;

    }


    /**
     * @see edu.teco.serverless.model.servicelayer.service.LambdaManagerFacade
     */
    public void deleteLambda(String nameOfLambda) throws RuntimeConnectException, TimeExceededException, IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        try {
            runTime = RuntimeController.getInstance();
            logger.info("runtTime is created.");
        } catch (IllegalAccessException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (InstantiationException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (ClassNotFoundException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (IOException e) {

            logger.error("This is error, " + e.getLocalizedMessage(), e);
            throw e;
        } catch (TimeExceededException e) {

            logger.error("Error, time limit exceeded.", e);
            throw e;
        }
        Identifier identifier = new Identifier(nameOfLambda);
        if (!runTime.lambdaExists(identifier)) {

            throw new LambdaNotFoundException();
        }
        runTime.deleteImage(identifier);
    }


}


