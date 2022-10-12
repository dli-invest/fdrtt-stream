import { getNormalizedPost } from "~/utils/getNormalizedPost";

const load = async function () {
  const posts = import.meta.glob("../data/posts/**/*.{md,mdx}", {
    eager: false,
  });

  const normalizedPosts = Object.keys(posts).map(async (key) => {
    const post = await posts[key];
    return await getNormalizedPost(post);
  });

  let results = (await Promise.all(normalizedPosts)).sort(
    (a, b) => new Date(b.pubDate).valueOf() - new Date(a.pubDate).valueOf()
  );

  // grab latest 1000 posts
  results = results.slice(0, 50);

  return results;
};

let _posts;

export const getPosts = async () => {
  _posts = _posts || load();

  return await _posts;
};
