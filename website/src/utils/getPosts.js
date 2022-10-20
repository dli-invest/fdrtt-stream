import { getNormalizedPost } from "../utils/getNormalizedPost";

const load = async function () {
  const posts = import.meta.glob("../data/posts/**/*.{md,mdx}", {
    eager: false,
  });

  let normalizedPosts = Object.keys(posts).map(async (key) => {
    const post = await posts[key];
    return await getNormalizedPost(post);
  });
  // remove null entries
  normalizedPosts = (await Promise.all(normalizedPosts)).filter(
    (post) => post !== null
  );

  let results = (await Promise.all(normalizedPosts)).sort(
    (a, b) => new Date(b.pubDate).valueOf() - new Date(a.pubDate).valueOf()
  );

  // grab latest 50 posts
  results = results.slice(0, 3);

  return results;
};

let _posts;

export const getPosts = async () => {
  _posts = _posts || load();

  return await _posts;
};
