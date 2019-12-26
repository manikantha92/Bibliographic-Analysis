package com.project.preprocessor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.project.preprocessor.Driver.*;


public final class AttributeMergingReducer extends Reducer<Text, Text, Text, Text> {

    public static final String NONAME = "NONAME";
    public static final String NOAUTHOR = "NOAUTHOR";
    public static final String NOYEAR = "NOYEAR";
    public static final String NOCITATIONS = "NOCITATIONS";
    public static final String NOVENUE = "NOVENUE";
    public static final String NOTERM = "NOTERM";
    private static final String TAB = "\t";
    private final Text result = new Text();

    public static String splitByCommaAndGetSecondElement(String word, Boolean addPublicationCount, String elseName) {
        StringBuilder sb = new StringBuilder();
        if (word != null) {
            String[] commaSeparatedWord = word.split(TAB);
            for (int i = 0; i < commaSeparatedWord.length - 1; i++) {
                if (commaSeparatedWord[i].split(",").length >= 2) {
                    String[] wordDets = commaSeparatedWord[i].split(",");
                    String wordFinal = addPublicationCount ? wordDets[0] + ":" + wordDets[1] + ":" + wordDets[2] + "," :
                            wordDets[0] + ":" + wordDets[1] + ",";
                    sb.append(wordFinal);
                }
            }
            String[] lastWordDets = commaSeparatedWord[commaSeparatedWord.length - 1].split(",");
            String lastWord = addPublicationCount ? lastWordDets[0] + ":" + lastWordDets[1] + ":" + lastWordDets[2] :
                    lastWordDets[0] + ":" + lastWordDets[1];
            sb.append(lastWord);
        } else {
            sb.append(elseName);
        }
        return sb.toString().trim();
    }

    public static String preProcessReducerString(String paperName, String paper, String author,
                                                 String venue, String term, String citationCounter) {
        StringBuilder sb = new StringBuilder();
        if (paperName != null) {
            String[] paperDets = paperName.split(TAB);
            for (int i = 0; i < paperDets.length; i++) {
                if (!paperDets[i].trim().equals("")) {
                    sb.append(paperDets[i]);
                    sb.append("|");
                }
            }
        } else {
            sb.append(NONAME);
            sb.append("|");
            sb.append(NOYEAR);
            sb.append("|");
        }
        int papersLength = 0;
        if (paper != null) {
            StringBuilder sPaper = new StringBuilder();
            String[] papers = paper.split(TAB);
            papersLength = papers.length;
            for (String paperI : papers) {
                if (paperI != null || !(paperI.trim()).equals("")) {
                    sPaper.append(paperI + ",");
                }
            }
            String paperStr = sPaper.toString().substring(1, sPaper.toString().length() - 1).trim();
            sb.append(paperStr);
        } else {
            sb.append(NOCITATIONS);
        }
        sb.append("|");
        sb.append(papersLength != 0 ? papersLength - 1 : papersLength);
        sb.append("|");
        sb.append(citationCounter != null ? citationCounter.trim() : 0);
        sb.append("|");
        sb.append(splitByCommaAndGetSecondElement(author, true, NOAUTHOR));
        sb.append("|");
        sb.append(venue != null ? (venue.split(",")[0] + ":" + venue.split(",")[1] + ":" +
                venue.split(",")[2]).trim() : NOVENUE);
        sb.append("|");
        sb.append(splitByCommaAndGetSecondElement(term, false, NOTERM));
        return sb.toString();
    }

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        String paper = null, author = null, venue = null, term = null, paperName = null, citationCounter = null;
        for (final Text val : values) {
            String valArr[] = val.toString().split(TAB);
            String type = valArr[0];
            String text = val.toString().substring(type.length());
            if (val != null) {
                switch (type) {
                    case PAPER:
                        paper = text;
                        break;
                    case TERM:
                        term = text;
                        break;
                    case CONF:
                        venue = text;
                        break;
                    case AUTHOR:
                        author = text;
                        break;
                    case PAPERNAME:
                        paperName = text;
                        break;
                    case CITATIONCOUNTER:
                        citationCounter = text;
                        break;
                }
            }
        }
        result.set(preProcessReducerString(paperName, paper, author, venue, term, citationCounter));
        if (paperName != null && author != null && venue != null && term != null) {
            if (paper != null) {
                context.getCounter(Counter.TOTAL_COMPLETE_DATASET).increment(1);
            } else {
                context.getCounter(Counter.DATASET_WITHOUT_CITATIONS).increment(1);
            }
        }
        context.write(key, result);
    }

    public static enum Counter {
        TOTAL_COMPLETE_DATASET,
        DATASET_WITHOUT_CITATIONS,
    }
}
