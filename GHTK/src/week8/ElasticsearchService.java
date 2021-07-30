package week8;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchService {


    public static void main(String[] args) throws IOException {
        String input = "sáng";

        // init connect
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("10.140.0.13",9200,"http")));


        // create request
        SearchRequest searchRequest = new SearchRequest("suggestion_haiht34");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders
                .completionSuggestion("suggest_title")
                .text(input);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);

        // create response
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        Suggest suggest = searchResponse.getSuggest();

        List<String> suggestions = searchResponse.getSuggest()
                .filter(CompletionSuggestion.class).stream()
                .map(CompletionSuggestion::getOptions)
                .flatMap(Collection::stream)
                .map(option -> option.getText().string())
                .distinct()
                .collect(Collectors.toList());

        System.out.println(suggestions.toString());
        client.close();
    }
}