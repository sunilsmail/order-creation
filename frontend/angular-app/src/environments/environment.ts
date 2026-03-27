export const environment = {
  // Use localhost when running locally, gateway when in Docker
  apiBaseUrl: typeof window !== 'undefined' && window.location.hostname === 'localhost' 
    ? 'http://localhost:3000' 
    : 'http://gateway:3000',
  socketUrl: typeof window !== 'undefined' && window.location.hostname === 'localhost'
    ? 'http://localhost:3000'
    : 'http://gateway:3000'
};
