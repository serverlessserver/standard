package edu.teco.serverless.model.converter;


import edu.teco.serverless.model.exception.messages.SemanticRequestException;
import edu.teco.serverless.model.lambda.Code;
import edu.teco.serverless.model.lambda.ExecuteConfig;
import edu.teco.serverless.model.lambda.Identifier;
import edu.teco.serverless.model.lambda.Lambda;
import edu.teco.serverless.model.lambda.Language;
import edu.teco.serverless.model.lambda.Library;
import edu.teco.serverless.model.lambda.Parameter;
import edu.teco.serverless.model.lambda.RunCycles;
import edu.teco.serverless.model.lambda.RuntimeAttributes;
import edu.teco.serverless.model.messages.ExecuteLambdaRequest;
import edu.teco.serverless.model.messages.RuntimeAttributesRequest;
import edu.teco.serverless.model.messages.UploadLambdaRequest;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts request objects to model objects and vice versa.
 */

public class RequestServerConverter {
    final static Logger logger = Logger.getLogger(RequestServerConverter.class);
    static boolean hasName = true;
    static boolean hasNameInput = true;
    static boolean hasCode = true;
    static boolean hasLanguage = true;
    static boolean hasLibraries = true;
    static boolean hasRuntimeAttributes = true;

    /**
     * Converts RuntimeAttributesRequest object to RuntimeAttributes object.
     *
     * @param runtimeAttributesRequest JSON with description of runtimeAttributes to be uploaded.
     * @return RuntimeAttributes object.
     */
    public static RuntimeAttributes runtimeAttributesRequestToRuntimeAttributes(RuntimeAttributesRequest runtimeAttributesRequest) {
        RuntimeAttributes.Builder builderRuntime = RuntimeAttributes.newRuntimeAttributes();
        if (runtimeAttributesRequest == null) {
            hasRuntimeAttributes = false;
        } else {

            if (runtimeAttributesRequest.getCode() != null && !runtimeAttributesRequest.getCode().equals("")) {
                builderRuntime.code(new Code(runtimeAttributesRequest.getCode()));
                logger.info("code is initialized.");
            } else {
                hasCode = false;

            }
            if (runtimeAttributesRequest.getLanguage() != null && !runtimeAttributesRequest.getLanguage().equals("")) {
                builderRuntime.language(new Language(runtimeAttributesRequest.getLanguage()));
                logger.info("language is initialized.");
            } else {
                hasLanguage = false;
            }
            List<Library> listLib = new ArrayList<>();
            if (runtimeAttributesRequest.getLibraries() != null) {

                for (int i = 0; i < runtimeAttributesRequest.getLibraries().size(); i++) {
                    listLib.add(new Library(runtimeAttributesRequest.getLibraries().get(i)));
                }
            } else {


                hasLibraries = false;

            }
            builderRuntime.libraries(listLib);
            logger.info("libraries are initialized.");
            return builderRuntime.build();
        }
        return null;
    }

    /**
     * Converts UploadLambdaRequest object to Lambda object.
     *
     * @param uploadLambdaRequest JSON with description of function to be uploaded.
     * @return lambda.
     */

    public static Lambda uploadRequestToLambda(UploadLambdaRequest uploadLambdaRequest) {
        Lambda.Builder builderLambda = Lambda.newLambda();
        List<String> exceptionMessages = new ArrayList<>();

        if (uploadLambdaRequest.getName() != null) {
            if (!uploadLambdaRequest.getName().equals("")) {
                builderLambda.name(new Identifier(uploadLambdaRequest.getName()));
                logger.info("name is initialized.");
            } else {
                hasNameInput = false;
            }
        } else {
            hasName = false;
        }
        RuntimeAttributes runtimeAttributes = runtimeAttributesRequestToRuntimeAttributes(uploadLambdaRequest.getRuntimeAttributes());

        logger.info("runtimeAttributes is created.");

        if (!hasName && !hasRuntimeAttributes) {
            setToTrue();
            throw new SemanticRequestException(" has wrong format");

        } else {

            if (!hasName || !hasNameInput) {
                exceptionMessages.add("name");

            }
            if (!hasRuntimeAttributes) {
                exceptionMessages.add("runtimeAttributes");
            } else {
                if (!hasCode) {
                    exceptionMessages.add("code");

                }
                if (!hasLanguage) {
                    exceptionMessages.add("language");

                }
            }

            if (exceptionMessages.size() != 0) {
                setToTrue();
                throw new SemanticRequestException(arrayToString(exceptionMessages));
            }

        }
        builderLambda.runtimeAttributes(runtimeAttributes);

        return builderLambda.build();
    }

    /**
     * Converts ExecuteLambdaRequest object to ExecuteConfig object.
     *
     * @param executeRequest JSON with description of execution's features .
     * @return ExecuteConfig Object.
     */
    public static ExecuteConfig executeRequestToExecuteConfig(ExecuteLambdaRequest executeRequest) {
        ExecuteConfig.Builder builder = ExecuteConfig.newExecuteConfig();
        if (executeRequest.getTimes() == 0) {
            builder.runCycles(new RunCycles(1));
            logger.info("runCycles are initialized.");
        }
        if (executeRequest.getTimes() >= 1) {
            builder.runCycles(new RunCycles(executeRequest.getTimes()));
            logger.info("runCycles are initialized.");
        }
        List<Parameter> listParam = new ArrayList<>();
        if (executeRequest.getParameters() != null
                ) {
            for (int i = 0; i < executeRequest.getParameters().size(); i++) {
                listParam.add(new Parameter(executeRequest.getParameters().get(i)));
            }
        }
        builder.parameterList(listParam);
        logger.info("parameterList is initialized.");
        return builder.build();
    }

    /**
     * Converts RuntimeAttributes object to RuntimeAttributesRequest object .
     *
     * @param runtimeAttributes runtimeAttributes to be converted.
     * @return RuntimeAttributesRequest object.
     */
    public static RuntimeAttributesRequest runtimeAttributesToRuntimeAttributesRequest(RuntimeAttributes runtimeAttributes) {
        RuntimeAttributesRequest.Builder builder = RuntimeAttributesRequest.newRuntimeAttributesRequest();
        if (runtimeAttributes.getCode() != null) {
            builder.code(runtimeAttributes.getCode().getCode());
        }
        if (runtimeAttributes.getLanguage() != null) {
            builder.language(runtimeAttributes.getLanguage().getLanguage());

        }
        List<String> list = new ArrayList<>();
        if (runtimeAttributes.getLibraries() != null) {
            for (int i = 0; i < runtimeAttributes.getLibraries().size(); i++) {
                list.add(runtimeAttributes.getLibraries().get(i).getLibrary());
            }
        }
        return builder.libraries(list).build();
    }

    /**
     * Converts Lambda object to UploadLambdaRequest object .
     *
     * @param lambda lambda to be converted.
     * @return UploadLambdaRequest object.
     */
    public static UploadLambdaRequest lambdaToUploadRequest(Lambda lambda) {
        UploadLambdaRequest.Builder builder = UploadLambdaRequest.newUploadLambdaRequest();
        RuntimeAttributesRequest runtimeAttributesRequest = runtimeAttributesToRuntimeAttributesRequest(lambda.getRuntimeAttributes());
        if (lambda.getName() != null) {
            builder.name(lambda.getName().getIdentifier());
        }

        return builder.runtimeAttributes(runtimeAttributesRequest).build();
    }

    /**
     * Converts array of exception´s messages to string type.
     *
     * @param arr array to be converted.
     * @return string of exceptions messages.
     */
    public static String arrayToString(List<String> arr) {
        String result = " has no ";
        String comma = ", ";
        if (arr.size() == 1) {
            result = result + arr.get(0);
        } else {
            for (int i = 0; i < arr.size() - 1; i++) {
                result = result + arr.get(i) + comma;
            }
            result = result + arr.get(arr.size() - 1);
        }
        return result;
    }

    /**
     * Sets all attributes to true.
     * Made for usage in other classes.
     */
    public static void setToTrue() {

        hasName = true;
        hasCode = true;
        hasLanguage = true;
        hasLibraries = true;
        hasRuntimeAttributes = true;
        hasNameInput = true;
    }


}

