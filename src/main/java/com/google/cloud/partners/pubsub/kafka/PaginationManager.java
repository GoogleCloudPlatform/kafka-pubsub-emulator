package com.google.cloud.partners.pubsub.kafka;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableList;
import java.util.Base64;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

public class PaginationManager<T> {

  private static final int DEFAULT_PAGINATION_SIZE = 20;

  private NavigableMap<String, T> cursor;

  private ImmutableList<T> result;

  public PaginationManager(Collection<T> data, Function<T, String> keyFunction) {
    this.cursor =
        data.stream()
            .collect(
                toMap(keyFunction, identity(), (oldValue, newValue) -> newValue, TreeMap::new));
  }

  public ImmutableList<T> paginate(int pageSize, String pageToken) {
    this.cursor = this.cursor.tailMap(decodePageToken(pageToken), false);
    this.result =
        this.cursor
            .values()
            .stream()
            .limit(pageSize > 0 ? pageSize : DEFAULT_PAGINATION_SIZE)
            .collect(ImmutableList.toImmutableList());
    return this.result;
  }

  public String getNextToken(Function<T, String> keyFunction) {
    if (this.cursor.values().size() > this.result.size()) {
      T last = this.result.get(this.result.size() - 1);
      return encodePageToken(keyFunction.apply(last));
    }
    return "";
  }

  private String encodePageToken(String token) {
    return new String(Base64.getEncoder().encode(token.getBytes()));
  }

  private String decodePageToken(String encodedToken) {
    return new String(Base64.getDecoder().decode(encodedToken.getBytes()));
  }
}
